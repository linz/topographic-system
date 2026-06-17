# Topographic System Schema Utils

## Step 1

Making a change to the schema - update / add to the require yml schema constructs under **src\features\*.tsp**.

**src\common.tsp** - contains the EMUMs for each field that uses them. Review if change is needed.

**src\main.tsk** - links the individual schema files - if removing or adding this file list need to be updated. References file under src\features.

**Example element to review**

jsonSchema and model contain names use - check no changes required >

@jsonSchema("bridge_line")
@extension("additionalProperties", false)
model bridge_line {

Check fields and related allow values. Enum typically in common.

bridge_use2?: Topo.BridgeUse2Enum

## Step 2

Update the json file - This can be done by running the commands

create json [Note: can be edited is small simple change]

> tsp compile .

If this does not run consider

> npx tsp compile .

depending on where you run this from, and how your tsp is installed; You may need to npx tsp compile ./packages/schema/src/
or just tsp compile . (dot for current folder) should be enough.

Then run **oxfmt** to reformat the json files

Other ways of running

> npx oxfmt .

> npx oxfmt --help
