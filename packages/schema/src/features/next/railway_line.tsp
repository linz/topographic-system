import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/railway_line")
@extension("additionalProperties", false)
/**
 * All mainline railway lines are held in the Topo50 data and shown on the Topo50 printed maps. 
 * Where a railway line is located close to a road, the line held in the data and shown on the printed map 
 * may be offset from the road sufficient that the two symbols are recognisable at 1:50,000.
 *
 * Multiple sidings may be held in the data and shown on the printed maps as a single feature
 * 
 * @example Reference Spec
 * ![Reference Spec](https://docs.topo.linz.govt.nz/data-dictionary/reprspecs/railway_cl.gif)
 *
 * @example Railway line
 * ![Railway Line](https://docs.topo.linz.govt.nz/data-dictionary/diagrams/railway_cl.gif)
 * 
 * @example Aerial view
 * ![Aerial View](https://docs.topo.linz.govt.nz/data-dictionary/airphotos/railway_cl.gif)
 */
model railway_line {
  ...TopographicFeature;
  
  type: "railway";
  subtype: railway_subtype | null;
  track_type: railway_track_type | null;
  vehicle_type: railway_vehicle_type | null;
  status: railway_status | null;
  name: string | null;

  theme: "transport";
  ...TopographicMetadata;
  ...GeoParquet
}

enum railway_subtype {
  Siding: "siding"
}

enum railway_track_type {
  Single: "single",
  Multiple: "multiple"
}

enum railway_vehicle_type {
  Train: "train",
  Tram: "tram", 
  /**
   * A decommissioned railway line with adapted motorised golf carts, or rail bicycles designed for tourist use
   */
  RailCart: "rail_cart",
  Cablecar: "cablecar"
}

enum railway_status {
  Disused: "disused",
}

