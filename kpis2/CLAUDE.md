# CLAUDE.md - Agent Instructions

## DBT Commands
- Run all models: `dbt run`
- Run specific model: `dbt run --select model_name`
- Test all models: `dbt test`
- Test specific model: `dbt test --select model_name`
- Generate docs: `dbt docs generate`
- Compile SQL: `dbt compile`
- Clean project: `dbt clean`
- Set variables: `dbt run --vars '{"start_date": "2023-01-01", "end_date": "2023-12-31"}'`

## Code Style Guidelines
- Use snake_case for all model and column names
- Include header comments in SQL files explaining KPI purpose and calculation
- Use CTEs for readability and modularity
- Reference sources with `{{ source('Combined Source', 'table_name') }}` syntax
- Always provide default values for variables: `{{ var('variable_name', 'default_value') }}`
- Handle division by zero with CASE statements or `nullif()`
- Each KPI model requires a matching YAML file for documentation
- Document models with descriptions and column-level metadata
- Use consistent indentation in SQL files (2 spaces)
- Format complex CASE statements with clear indentation
- Add tests for key metrics (not_null, numeric, etc.)

## Project Structure
- Source tables defined in models/sources.yml under 'Combined Source'
- KPI models stored in models/kpi/ directory
- All models reference source tables directly (not intermediate models)