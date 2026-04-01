import type { StacItem, StacCollection, StacCatalog, StacProvider } from 'stac-ts';
export const StacBasic = {
  item: createBasicStacItem,
  catalog: createBasicStacCatalog,
  collection: createBasicStacCollection,
};

export const Providers: StacProvider[] = [
  { name: 'Land Information New Zealand', url: 'https://www.linz.govt.nz/', roles: ['processor', 'host'] },
];

function createBasicStacItem(collectionId: string, id = '', date = new Date()): StacItem {
  return {
    id,
    type: 'Feature',
    collection: collectionId,
    stac_version: '1.0.0',
    stac_extensions: [],
    geometry: null,
    bbox: [],
    links: [],
    properties: {
      datetime: date.toISOString(),
    },
    assets: {},
  };
}

function createBasicStacCollection(id = '', date = new Date()): StacCollection {
  return {
    type: 'Collection',
    stac_version: '1.0.0',
    id: id,
    description: '',
    extent: {
      spatial: { bbox: [[0, 0, 0, 0]] },
      temporal: { interval: [[date.toISOString(), null]] },
    },
    links: [],
    license: 'CC-BY-4.0',
    created: date,
    updated: date,
    providers: Providers,
    stac_extensions: [],
    summaries: {},
  };
}

function createBasicStacCatalog(id = '', date = new Date()): StacCatalog {
  return {
    type: 'Catalog',
    stac_version: '1.0.0',
    stac_extensions: [],
    id,
    title: '',
    description: '',
    links: [],
    created: date,
    updated: date,
  };
}
