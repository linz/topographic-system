import type { QgisLayerDef } from '@linzjs/topographic-system-shared';

function hasQuery(layer: QgisLayerDef): boolean {
  return (layer.options ?? []).find((f) => f.key === 'subset') != null;
}

/**
 * Find a layer in the project.
 *
 * When `explicitName` is provided the layer whose source matches it exactly is returned
 * otherwise the first layer whose source ends with `suffix` is used.
 *
 * @param label human readable name used in error messages, e.g. "Map sheet"
 * @param strategyCollections optional map of layer name → strategy collection URL to override the layer's source
 */
function findQgisLayer(layers: QgisLayerDef[], suffix: string, label: string, explicitName?: string): QgisLayerDef {
  if (explicitName != null) {
    // add .parquet if there is no extension
    const searchName = explicitName.includes('.') ? explicitName : `${explicitName}.parquet`;
    const layer = layers.find((f) => f.source === searchName && hasQuery(f) === false);
    if (layer) return layer;
    throw new Error(`${label} source layer not found: "${explicitName}"`);
  }

  const layer = layers.find((f) => f.source.endsWith(suffix) && hasQuery(f) === false);
  if (layer == null) throw new Error(`No ${label.toLowerCase()} layer ending with "${suffix}" found`);
  return layer;
}

/** Attempt to find the carto text layer */
export function getQgisCartoTextLayer(layers: QgisLayerDef[], cartoTextLayerName?: string): QgisLayerDef {
  return findQgisLayer(layers, 'carto_text.parquet', 'Carto text', cartoTextLayerName);
}

/** Attempt to find a MapSheet metadata layer */
export function getQgisMapSheetDataset(layers: QgisLayerDef[], mapSheetLayerName?: string): QgisLayerDef {
  return findQgisLayer(layers, 'map_sheet.parquet', 'Map sheet', mapSheetLayerName);
}
