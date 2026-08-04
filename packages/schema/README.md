# Topographic System Schema Changes

## Step 1

Making a change to the schema - update / add to the require yml schema constructs under **src\features\*.tsp**.

Files are updated in **NEXT** until **manual** release process which will move the tsp and json files back into the main folder.

**A)** copy the file(s) to change into the **src\features\next** folder

**B)** main.tsp - in next this just contains the local files - on release full file will need review (if new layer added or old ones deleted - likely to be rare). Ensure the moved file is added to **next/main.tsp**

**C)** edit file as required. IMPORTANT - the schema link needs to point to next @jsonSchema("next/railway_line")


**D)** Review check

**src\features\common.tsp** - contains the EMUMs for each field that uses them. Review if change is needed. THis now only contain widely common enums.

**src\features\main.tsp** - links the individual schema files - if removing or adding this file list need to be updated. References file under src\features.

**Example element to review**

@jsonSchema("bridge_line") -> @jsonSchema("next\bridge_line")

Check fields and related allow values. Enum typically in same schema file and lowercase..

example -> use2: use2

## Step 2

Update the json files - This can be done by running the commands below

create json [Note: should alway be run so nothing is missed]

Run **Next**

Assuming running from topographic-system folder

format tsp files...

> `npx tsp format "packages/schema/src/**/*.tsp"`

or single file

> `npx tsp format "packages/schema/src/**/place_point.tsp"`

compile...

> npx tsp compile ./packages/schema/src/next.tsp --config ./packages/schema/tspconfig.next.yaml

Run Main - this is **when full release** is required

linux

> tsp compile ./packages/schema/src/features/main.tsp --config ./packages/schema/tspconfig.yaml

If this does not run in your environment use npx (requires installation)

> npx tsp compile ./packages/schema/src/features/main.tsp --config ./packages/schema/tspconfig.yaml

depending on where you run this from, and how your tsp is installed; You may need to

> npx tsp compile ./packages/schema/src/
> or just tsp compile . (dot for current folder) should be enough.

## JSON format

Then move to the json output folder - topographic-system/schema/next or topographic-system/schema

Then run **oxfmt** to reformat the json files

Other ways of running

> cd schema  or cd schema/next

> npx oxfmt .

for help..

> npx oxfmt --help
