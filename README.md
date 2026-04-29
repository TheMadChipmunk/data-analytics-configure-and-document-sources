## Context

You're building a data pipeline for Jaffle Shop, a coffee shop business. In the previous challenge, you created basic source definitions in `models/schema.yml`. Now you'll enhance these sources with rich documentation and data quality tests.

**New Challenge = New GitHub Repository:** Each challenge has its own repo for tracking completion. You'll push your existing work from the previous challenge to this new repo, then enhance it.

By the end of this challenge, you'll understand how to:

- Add `database` and `identifier` configuration to source definitions
- Decouple the names you write in SQL from the actual table names in the database
- Use `dbt parse` to catch YAML errors early
- Read compiled SQL to verify dbt resolved source references correctly

## Objective

Enhance your source definitions with `database` and `identifier` configuration, update your staging model to use the clean identifier, and verify the full source-to-model chain works end-to-end.

## Prerequisites

- Completed the previous challenge with a `jaffle_shop_dbt/` directory containing:
  - `dbt_project.yml` configured for jaffle_shop_dbt
  - `models/schema.yml` with basic source definitions (3 tables)
  - `models/staging/stg_customers.sql` using source() macro
  - `dev.duckdb` with raw data loaded

## 0. Copy Your Work from Previous Challenge

**Important:** Each challenge has its own directory. Copy your dbt project files from the previous challenge.

```bash
cp -rP ../../../{{ local_path_to("03-Data-Transformation/09-Data-Layers-And-Intro-DBT/02-Load-Data-And-First-Model") }}/jaffle_shop_dbt .

# Verify the symlink copied correctly
ls -l jaffle_shop_dbt/dev.duckdb
# Should show: dev.duckdb -> ../../../dbt-shared/dev.duckdb
```

Then commit so you have a clean starting point for this challenge:

```bash
git add jaffle_shop_dbt
git commit -m "Copied setup from previous challenge"
git push origin master
```

<details>
<summary markdown="span">**💡 Why does the database carry over automatically?**</summary>

The `dev.duckdb` file in your project is a symlink pointing to `../../../dbt-shared/dev.duckdb`. When you `cp -r` the project, the symlink is copied as-is. Because all challenge directories sit at the same depth under their unit folder, the relative path still resolves to the same shared database — no recreation needed. DBeaver stays connected without any path changes.

**Tip:** If you made mistakes in the previous challenge, this is your chance to fix them before continuing!

</details>

## 1. Navigate to Your dbt Project

Open the source definitions file you created in the previous challenge:

```bash
code jaffle_shop_dbt/models/schema.yml  # Open your existing schema file
```

**💡 Why are we modifying an existing file?**

In the previous challenge, you created source definitions with `schema: raw` already set. Now you're **enhancing** them with:

- `database: dev` so dbt can build fully qualified table references
- `identifier` mapping to decouple the clean names you write in SQL from the raw table names in the database

This is how real dbt projects evolve — with iterative improvements rather than one-time setup.

## 2. Enhance Your Source Definitions

### 2.1. Add Database and Identifier Mapping

Your `schema.yml` already has `schema: raw` from the previous challenge. Now add two more fields:

1. **database:** The DuckDB catalog name (`dev`) — needed to build a fully qualified table reference like `"dev"."raw"."raw_customers"`
2. **identifier:** Maps the clean name you'll use in SQL to the actual table name in the database (e.g., `customers` → `raw_customers`)

**Goal:** After this change you can write `{{ source('jaffle_shop', 'customers') }}` in your models instead of `{{ source('jaffle_shop', 'raw_customers') }}` — keeping model code clean and decoupled from raw naming conventions.

<details>
<summary markdown="span">**💡 Hint: YAML structure**</summary>

Add `database` at the source level and `identifier` for each table. Your `schema: raw` is already there:

```yaml
sources:
  - name: jaffle_shop
    database: ???        # DuckDB catalog name (stem of dev.duckdb)
    schema: raw          # Already set — the schema where your tables live
    tables:
      - name: customers
        identifier: ???  # The actual table name in the database
```

> **💡 `database: dev`** — this is the DuckDB **catalog name** (the stem of the `dev.duckdb` filename), not the file path. dbt uses it to build the fully qualified table reference `"dev"."raw"."raw_customers"`.

</details>

<details>
<summary markdown="span">**🔍 Need more help?**</summary>

The `identifier` key maps the friendly name to the actual table name:

- You want to use `{{ source('jaffle_shop', 'customers') }}` in your code (clean!)
- But the actual table is called `raw_customers` (with prefix)
- `identifier: raw_customers` makes this mapping

Apply `database: dev` once at the source level, and `identifier` for all three tables.

</details>

### 🧪 Checkpoint 1: Push Schema Configuration

**📍 In your terminal**, if you are not already in the challenge directory, navigate there now:

```bash
cd ..
```

Then run the checkpoint 1 tests:

```bash
pytest tests/test_schema_config.py -v
```

**If tests pass**, commit your schema configuration:

```bash
git add jaffle_shop_dbt/models/schema.yml
git commit -m "Add database and identifier mapping"
git push origin master
```

`schema.yml` is locked in. Next you'll see why these changes matter — by watching the staging model break and then fixing it.

### 2.2. Understanding Sources vs Models

**What are sources?**

Sources represent **data you don't control** - tables created by:

- Application databases
- ETL tools (Fivetran, Airbyte)
- Manual data loads
- External systems

You **read** from sources but never modify them.

**What are models?**

Models represent **transformations you create** - tables/views built by dbt:

- Staging models (clean raw data)
- Intermediate models (business logic)
- Mart models (final analytics tables)

You **write** models and control their logic.

**Key Difference:**

```sql
-- ❌ Don't do this in production
SELECT * FROM raw.raw_customers  -- Direct table reference

-- Do this
SELECT * FROM {{ source('jaffle_shop', 'customers') }}  -- Source reference
```

**Why use sources?**

- Centralized data location management
- Easy to update if table names change
- Automatic lineage tracking
- Foundation for data freshness checks
- Clear boundary between "external" and "our" data

### 2.3. Verify Configuration

**📍 In your terminal**, navigate to your dbt project:

```bash
cd jaffle_shop_dbt
```

Check your updated YAML parses correctly before running any models:

```bash
dbt parse
```

You'll get a pretty long output. That is normal, the thing we are interested in: are there any errors? How can you know?

- Option 1: Scan the output for any mentions of "*error*". (You will see some "*warnings*", you can disregard those for now.)
- Option 2: Check the start of the new prompt in your terminal (the new line that appears after the output of the command). Do you see a green arrow? Then the previous command succeeded (i.e. no errors). Is the arrow red? Then the previous command failed (i.e. there are errors).

**What you are hoping to see:**

- No compilation errors about sources
- Clean completion — if you see errors (not just informational lines), check:
  - YAML indentation (use spaces, not tabs!)
  - Colons have spaces after them: `name: value` not `name:value`
  - Dashes align properly under each list

**⚠️ Teaching moment:** Your YAML is valid, but since you renamed the source table references (from `raw_customers` to `customers`), your staging model now points to a name that doesn't exist yet in the model code. Try running it:

```bash
dbt run --select stg_customers
```

What error do you get? Read it carefully — dbt is telling you exactly what it can't find. This is what happens in real dbt projects when you rename or restructure sources: downstream models break until you update their references.

**Now fix the reference. 📝 In VS Code**, open and update your staging model:

```bash
code models/staging/stg_customers.sql
```

**Change the source reference:**

```sql
-- BEFORE (without identifier mapping)
SELECT * FROM {{ source('jaffle_shop', 'raw_customers') }}

-- AFTER (with identifier mapping - cleaner!)
SELECT * FROM {{ source('jaffle_shop', 'customers') }}
```

**💡 Why this change?** You added the `identifier: raw_customers` mapping in section 2.1, which allows you to use the cleaner name `customers` in your code while dbt knows the actual table is `raw_customers`.

Don't forget to **💾 save the file**

### 🧪 Checkpoint 2: Push Updated Staging Model

**📍 In your terminal**, if you are not already in the challenge directory, navigate there now:

```bash
cd ..
```

Then run the checkpoint 2 tests:

```bash
pytest tests/test_analysis.py -v
```

**If tests pass**, commit your updated staging model:

```bash
git add jaffle_shop_dbt/models/staging/stg_customers.sql
git commit -m "Update staging model to use clean source identifier"
git push origin master
```

The staging model now uses the clean identifier and dbt can resolve it end-to-end. Next you'll run it and inspect what SQL dbt actually sends to the database.

## 3. Inspect What dbt Builds

### 3.1. Run Your Staging Model

Now that the source configuration and staging model reference are both correct, run the model and examine what dbt actually generates.

**�️ Note:** If DBeaver is connected to `jaffle_shop_shared`, disconnect it first (right-click connection → Disconnect) to avoid a database lock error.

**�📍 In your terminal**, navigate to your dbt project if not already there:

```bash
cd jaffle_shop_dbt
```

Run the staging model:

```bash
dbt run --select stg_customers
```

<details>
<summary markdown="span">**Expected output** (version numbers will vary)</summary>

```plaintext
Running with dbt=1.10.0
Registered adapter: duckdb=1.10.0
Found 1 model, 3 sources, 443 macros
Concurrency: 4 threads (target='dev')

1 of 1 START sql view model main.stg_customers ................................ [RUN]
1 of 1 OK created sql view model main.stg_customers .......................... [OK in 0.05s]

Finished running 1 view model in 0 hours 0 minutes and 0.26 seconds.

Completed successfully
```

</details>

Now open the compiled SQL that dbt generated:

```bash
code target/compiled/jaffle_shop_dbt/models/staging/stg_customers.sql
```

<details>
<summary markdown="span">**Expected compiled output**</summary>

```sql
SELECT * FROM "dev"."raw"."raw_customers"
```

</details>

**🎓 Key Learning:** dbt replaced `{{ source('jaffle_shop', 'customers') }}` with the fully qualified table name `"dev"."raw"."raw_customers"`.

This is exactly what your three configuration entries control:

- `database: dev` → `"dev"` prefix
- `schema: raw` → `"raw"` schema
- `identifier: raw_customers` → actual table name

The macro is the bridge between the clean name you write (`customers`) and where the data actually lives (`raw.raw_customers`).

## 🎉 Challenge Complete

Source definitions are fully configured and your staging model runs successfully.

**Key takeaways:**

- `schema: raw` was already set from the previous challenge — it tells dbt which DuckDB namespace to look in for raw tables
- `database: dev` provides the catalog name to build fully qualified table references
- `identifier` decouples the name you write in SQL from the actual table name in the database
- When you change source identifiers, downstream model code breaks until you update it — dbt makes these issues visible immediately
