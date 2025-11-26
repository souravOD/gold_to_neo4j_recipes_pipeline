# Pipeline: recipes

Scope: Ingest recipe-domain changes from Supabase Gold v3 into Neo4j v3 using a Python worker. This repo is self-contained (no shared code).

Supabase source tables: recipes, nutrition_facts (recipe), recipe_ingredients, recipe_ratings, cuisines
Neo4j labels touched: Recipe, RecipeNutritionValue, Ingredient, Product, Cuisine
Neo4j relationships touched: HAS_NUTRITION_VALUE, OF_NUTRIENT, USES_INGREDIENT, USES_PRODUCT, HAS_CUISINE

How it works
- Outbox-driven: worker polls `outbox_events` (aggregate_type='recipe'), locks with `SKIP LOCKED`, routes to upsert.
- Upsert logic: reloads recipe core + nutrition_facts + recipe_ingredients + aggregated ratings; rebuilds ingredient, product (if present), cuisine edges idempotently.
- Deletes: if event op=DELETE and row absent in Supabase, `DETACH DELETE` the Recipe node; otherwise treat as upsert.

Run
- Install deps: `pip install -r requirements.txt`
- Configure env: copy `.env.example` → `.env` and fill Postgres/Neo4j credentials.
- Start worker: `python -m src.workers.runner`

Folders
- docs/: domain notes, Cypher patterns, event routing
- src/: config, adapters (supabase, neo4j, queue), domain models/services, pipelines (aggregate upserts), workers (runners), utils
- tests/: placeholder for unit/integration tests
- ops/: ops templates (docker/env/sample cron jobs)
