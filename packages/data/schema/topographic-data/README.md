This is a working folder to show how to make use of pydantic to generate schemas and run schema validation.

See: readme_pydantic_validation.md for example of generic capabilities. AI generated.

Pydatnic Docs - https://docs.pydantic.dev/


Primary schema source is from the topographic-system repo - schema or schema/next folder with one JSON file per layer. Files can be copied locally if need to merge next and main. The generate_model script points to the json folder.

This is used by the **pydantic_models.py** to create pydantic version models. Use the **generate_models.py** script to update the classes.

### Step 1: Run: generate_models.py

The results are:

**pydantic_model_classes.py** is a full model set of classes.

**validate_buildings_example.py** - show how to validate against the model - build building as example. Has methodology and sample pass/fail datasets as examples.


### Step 2: Run: generate_model_html.py

**generate_model_html.py** - creates a html version of the schema. Uses the pydantic model.

In **examples** folder is output HTML and MD version and a example openAPI 3.0.3 version