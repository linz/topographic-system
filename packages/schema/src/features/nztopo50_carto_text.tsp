import "@typespec/json-schema";
import "../common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("nztopo50_carto_text")
@extension("additionalProperties", false)
model nztopo50_carto_text {
  ...Topo.TopoIdentity;
  /** topo_id (UUID) of the example feature. */
  example_point_id?: string | null;
  full_text?: string | null;
  text_bend?: int32 | null;
  text_char_spacing_distance?: int32 | null;
  text_colour?: int32 | null;
  text_font?: Topo.TextFontEnum | null;
  text_height?: float64 | null;
  text_orientation?: float64 | null;
  text_placement?: int32 | null;
  text_size_type?: int32 | null;
  text_stretch_length?: int32 | null;
  text_string?: string | null;
  text_word_spacing_distance?: int32 | null;
  font?: Topo.FontEnum | null;
  style?: Topo.StyleEnum | null;
  colour?: Topo.ColourEnum | null;
  size?: float64 | null;
  placement?: Topo.PlacementEnum | null;
  offset?: float64 | null;
  textanchor?: Topo.TextanchorEnum | null;
  labelanchor?: float64 | null;
  charplace?: Topo.CharplaceEnum | null;
  chardistance?: float64 | null;
  /** WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. */
  @extension("not", #{ type: "null" })
  geometry: unknown;
  bbox?: Topo.BBox;
}
