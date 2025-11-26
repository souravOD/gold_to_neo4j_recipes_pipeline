from typing import Dict, List, Optional

from src.adapters.neo4j.client import Neo4jClient
from src.adapters.supabase import db as pg
from src.config.settings import Settings
from src.domain.models.events import OutboxEvent
from src.utils.logging import configure_logging


class RecipePipeline:
    """Upserts Recipe aggregates (recipe + nutrition + ingredients + cuisine + ratings) into Neo4j."""

    def __init__(self, settings: Settings, pg_pool: pg.PostgresPool, neo4j: Neo4jClient):
        self.settings = settings
        self.pg_pool = pg_pool
        self.neo4j = neo4j
        self.log = configure_logging("recipe_pipeline")

    # ===================== DATA LOADERS =====================
    def load_recipe(self, conn, recipe_id: str) -> Optional[Dict]:
        sql = """
        SELECT r.*, c.id AS cuisine_id, c.name AS cuisine_name, c.code AS cuisine_code
        FROM recipes r
        LEFT JOIN cuisines c ON c.id = r.cuisine_id
        WHERE r.id = %s;
        """
        return pg.fetch_one(conn, sql, (recipe_id,))

    def load_nutrition_facts(self, conn, recipe_id: str) -> List[Dict]:
        sql = """
        SELECT nf.id, nf.nutrient_id, nf.amount, nf.unit, nf.per_amount, nf.per_amount_grams,
               nf.percent_daily_value, nf.data_source, nf.confidence_score, nf.measurement_date
        FROM nutrition_facts nf
        WHERE nf.entity_type = 'recipe' AND nf.entity_id = %s;
        """
        return pg.fetch_all(conn, sql, (recipe_id,))

    def load_ingredients(self, conn, recipe_id: str) -> List[Dict]:
        sql = """
        SELECT ri.ingredient_id AS id, i.name, ri.quantity, ri.unit, ri.quantity_normalized_g,
               ri.ingredient_order, ri.preparation_note, ri.is_optional, ri.product_id
        FROM recipe_ingredients ri
        LEFT JOIN ingredients i ON i.id = ri.ingredient_id
        WHERE ri.recipe_id = %s
        ORDER BY ri.ingredient_order NULLS LAST;
        """
        return pg.fetch_all(conn, sql, (recipe_id,))

    def load_ratings(self, conn, recipe_id: str) -> Dict:
        sql = """
        SELECT COALESCE(AVG(rating), 0) AS avg_rating,
               COUNT(*) AS rating_count
        FROM recipe_ratings
        WHERE recipe_id = %s;
        """
        row = pg.fetch_one(conn, sql, (recipe_id,))
        return row or {"avg_rating": 0, "rating_count": 0}

    # ===================== CYPHER =====================
    def _upsert_cypher(self) -> str:
        """Cypher script to rebuild all recipe relationships idempotently."""
        return """
        MERGE (r:Recipe {id: $recipe.id})
        SET r.title = $recipe.title,
            r.description = $recipe.description,
            r.meal_type = $recipe.meal_type,
            r.difficulty = $recipe.difficulty,
            r.prep_time_minutes = $recipe.prep_time_minutes,
            r.cook_time_minutes = $recipe.cook_time_minutes,
            r.total_time_minutes = $recipe.total_time_minutes,
            r.servings = $recipe.servings,
            r.image_url = $recipe.image_url,
            r.source_url = $recipe.source_url,
            r.source_type = $recipe.source_type,
            r.instructions = $recipe.instructions,
            r.percent_calories_protein = $recipe.percent_calories_protein,
            r.percent_calories_fat = $recipe.percent_calories_fat,
            r.percent_calories_carbs = $recipe.percent_calories_carbs,
            r.avg_rating = $ratings.avg_rating,
            r.rating_count = $ratings.rating_count,
            r.updated_at = datetime($recipe.updated_at),
            r.created_at = datetime($recipe.created_at)

        // Cuisine
        WITH r
        OPTIONAL MATCH (r)-[oldCuisine:HAS_CUISINE]->(:Cuisine)
        DELETE oldCuisine;
        WITH r
        FOREACH (_ IN CASE WHEN $recipe.cuisine_id IS NULL THEN [] ELSE [1] END |
          MERGE (c:Cuisine {id: $recipe.cuisine_id})
          SET c.name = $recipe.cuisine_name,
              c.code = $recipe.cuisine_code
          MERGE (r)-[:HAS_CUISINE]->(c)
        )

        // Ingredients
        WITH r
        OPTIONAL MATCH (r)-[oldIng:USES_INGREDIENT]->(:Ingredient)
        DELETE oldIng;
        WITH r
        OPTIONAL MATCH (r)-[oldProd:USES_PRODUCT]->(:Product)
        DELETE oldProd;
        WITH r, $ingredients AS ingredients
        UNWIND ingredients AS ing
          MERGE (i:Ingredient {id: ing.id})
          SET i.name = coalesce(ing.name, i.name)
          MERGE (r)-[ri:USES_INGREDIENT]->(i)
          SET ri.quantity = ing.quantity,
              ri.unit = ing.unit,
              ri.quantity_normalized_g = ing.quantity_normalized_g,
              ri.ingredient_order = ing.ingredient_order,
              ri.preparation_note = ing.preparation_note,
              ri.is_optional = ing.is_optional;
        WITH r, $ingredients AS ingredients
        UNWIND ingredients AS ingProd
          FOREACH (_ IN CASE WHEN ingProd.product_id IS NULL THEN [] ELSE [1] END |
            MERGE (p:Product {id: ingProd.product_id})
            MERGE (r)-[rp:USES_PRODUCT]->(p)
            SET rp.quantity = ingProd.quantity,
                rp.unit = ingProd.unit,
                rp.quantity_normalized_g = ingProd.quantity_normalized_g,
                rp.ingredient_order = ingProd.ingredient_order
          )

        // Nutrition values
        WITH r
        OPTIONAL MATCH (r)-[oldNv:HAS_NUTRITION_VALUE]->(nv:RecipeNutritionValue)
        DELETE oldNv, nv;
        WITH r, $nutrition_facts AS facts
        UNWIND facts AS nf
          MATCH (nd:NutrientDefinition {id: nf.nutrient_id})
          MERGE (nv:RecipeNutritionValue {id: nf.id})
          SET nv.amount = nf.amount,
              nv.unit = nf.unit,
              nv.per_amount = nf.per_amount,
              nv.per_amount_grams = nf.per_amount_grams,
              nv.percent_daily_value = nf.percent_daily_value,
              nv.data_source = nf.data_source,
              nv.confidence_score = nf.confidence_score,
              nv.measurement_date = nf.measurement_date
          MERGE (r)-[:HAS_NUTRITION_VALUE]->(nv)
          MERGE (nv)-[:OF_NUTRIENT]->(nd);
        """

    def _delete_cypher(self) -> str:
        return "MATCH (r:Recipe {id: $id}) DETACH DELETE r;"

    # ===================== OPERATIONS =====================
    def handle_event(self, event: OutboxEvent) -> None:
        with self.pg_pool.connection() as conn:
            recipe = self.load_recipe(conn, event.aggregate_id)

        if recipe is None:
            if event.op.upper() == "DELETE":
                self.log.info("Deleting recipe from graph", extra={"id": event.aggregate_id})
                self.neo4j.write(self._delete_cypher(), {"id": event.aggregate_id})
            else:
                self.log.warning(
                    "Recipe missing in Supabase; skipping upsert",
                    extra={"id": event.aggregate_id, "op": event.op},
                )
            return

        with self.pg_pool.connection() as conn:
            nutrition = self.load_nutrition_facts(conn, event.aggregate_id)
            ingredients = self.load_ingredients(conn, event.aggregate_id)
            ratings = self.load_ratings(conn, event.aggregate_id)

        params = {
            "recipe": recipe,
            "nutrition_facts": nutrition,
            "ingredients": ingredients,
            "ratings": ratings,
        }
        self.neo4j.write(self._upsert_cypher(), params)
        self.log.info(
            "Upserted recipe aggregate",
            extra={
                "id": event.aggregate_id,
                "nutrition_values": len(nutrition),
                "ingredients": len(ingredients),
                "ratings": ratings,
            },
        )
