import "@typespec/json-schema";
import "./common.tsp";

using TypeSpec.JsonSchema;

@jsonSchema("next/nztopo50_carto_text")
@extension("additionalProperties", false)
model nztopo50_carto_text {
  ...ProductFeature;

  /** id (UUID) of the example feature. */
  example_point_id: string | null;
  full_text: string | null;
  text_bend: int32 | null;
  text_char_spacing_distance: int32 | null;
  text_colour: int32 | null;
  text_font: carto_textfont | null;
  text_height: float64 | null;
  text_orientation: float64 | null;
  text_placement: int32 | null;
  text_size_type: int32 | null;
  text_stretch_length: int32 | null;
  text_string: string | null;
  text_word_spacing_distance: int32 | null;
  font: carto_font | null;
  style: carto_style | null;
  colour: carto_colour | null;
  size: float64 | null;
  placement: carto_placement | null;
  offset: float64 | null;
  textanchor: carto_textanchor | null;
  labelanchor: float64 | null;
  charplace: carto_charplace | null;
  chardistance: float64 | null;

  ...GeoParquet
}

/* the empty string is temporarily while new fields getting updated */

enum carto_font {
    _: "",
  NimbusSansLinz: "Nimbus Sans LINZ",
}

enum carto_style {
  _: "",
  Italic: "Italic",
  Narrow: "Narrow",
  NarrowBold: "Narrow Bold",
  NarrowItalic: "Narrow Italic",
  Regular: "Regular",
}

enum carto_colour {
  _: "",
  Black: "black",
  ProcessBlue: "process_blue",
  Red: "red",
}

enum carto_placement {
  _: "",
  AL: "AL",
  BL: "BL",
  OL: "OL",
}

enum carto_textanchor {
  _: "",
  Centre: "centre",
  Left: "left",
  LeftWithSpace: "left ",  // Note trailing space; not a typo. TODO: Need to fix source data to remove this.
  Left062: "left 0.62",
  Right: "right",
}

enum carto_charplace {
  _: "",
  CharactersAtVertices: "CharactersAtVertices",
  StretchCharacterSpacingToFit: "StretchCharacterSpacingToFit",
  StretchWordSpacingToFit: "StretchWordSpacingToFit",
}

/* 
  The following fonts are not used in the NZTopo50 product in the future. May be removed when new font set up.
*/
enum carto_textfont {
  ATTriumItalic: "ATTrium-Italic",
  ATTriumMouCond: "ATTriumMou-Cond",
  ATTriumMouCondBold: "ATTriumMou-CondBold",
  ATTriumMouCondItalic: "ATTriumMou-CondItalic",
  ATTriumMouItalic: "ATTriumMou-Italic",
  ATTriumMouRegular: "ATTriumMou-Regular",
  CourierBoldOblique: "Courier Bold Oblique",
}