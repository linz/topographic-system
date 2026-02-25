import { basename } from 'path';
import type { StacAsset, StacCatalog, StacCollection, StacItem, StacLink } from 'stac-ts';

import { CliDate, CliInfo } from './cli.info.ts';
import { isMergeToMaster, isPullRequest, isRelease } from './github.ts';
import { logger } from './log.ts';
import { RootCatalogFile } from './stac.constants.ts';
import { createFileStatsFromStac } from './stac.factory.ts';

export function getSelfLink(stac: StacItem | StacCollection | StacCatalog): string {
  const selfLink = stac.links.find((link) => link.rel === 'self');
  if (selfLink === undefined) {
    logger.error({ stac }, 'STAC:SelfLinkUndefined');
    throw new Error('STAC self link is undefined');
  }
  return selfLink.href;
}

export function compareStacAssets(a: StacAsset | StacLink | undefined, b: StacAsset | StacLink | undefined): boolean {
  if (a == null && b == null) return true;
  if (a && b)
    return (
      a.href === b.href &&
      a.type === b.type &&
      a['file:checksum'] === b['file:checksum'] &&
      a['file:size'] === b['file:size']
    );
  return false;
}

export function determineAssetLocation(subdir: string, dataset: string, output: string, tag?: string): URL {
  if (!tag) {
    if (isMergeToMaster() || isRelease()) {
      tag = `year=${CliDate.slice(0, 4)}/date=${CliDate}`;
    } else if (isPullRequest()) {
      const ref = process.env['GITHUB_REF_NAME'] ?? '';
      const prMatch = ref.match(/(\d+)\/merge/); // TODO: Check if better with GITHUB_REF
      if (prMatch) {
        tag = `pull_request/pr-${prMatch[1]}`;
      } else {
        tag = `pull_request/unknown`;
      }
    } else {
      tag = `dev-${CliInfo.hash}`;
    }
  }
  logger.info(
    { subdir, dataset, tag, master: isMergeToMaster(), release: isRelease(), pr: isPullRequest() },
    'ToParquet:DetermineS3LocationContextVars',
  );
  return new URL(`${subdir}/${dataset}/${tag}/${basename(output)}`, RootCatalogFile);
}

/**
 * Ensure that the STAC child has a link to the specified STAC parent.
 * If the link is missing or incorrect, it will be added or updated.
 * If the child is a STAC Item, its collection ID will also be updated to match the parent's ID.
 *
 * @param stacChild - The STAC child (Item, Collection, or Catalog).
 * @param stacParent - The STAC parent (Collection or Catalog).
 *
 * @returns The updated STAC child with the correct parent link and collection ID (if applicable).
 */
export function addParentDataToChild(
  stacChild: StacItem | StacCollection | StacCatalog,
  stacParent: StacCollection | StacCatalog,
): StacItem | StacCollection | StacCatalog {
  const stacParentFile = getSelfLink(stacParent);
  const stacChildFile = getSelfLink(stacChild);
  const childIsItem = stacChild.type === 'Feature';
  const parentIsCollection = stacParent.type === 'Collection';
  if (childIsItem && parentIsCollection) {
    // Set Collection link and update Collection ID for Item
    const collectionLink = stacChild.links.find((link) => link.rel === 'collection');
    if (collectionLink === undefined) {
      stacChild.links.push({ rel: 'collection', href: stacParentFile, type: 'application/json' });
      stacChild.collection = stacParent.id;
      stacChild.properties.datetime = CliDate;
      logger.info(
        { stacChildFile, collection: stacChild.collection, stacParentFile },
        'STAC:ItemCollectionLinkAndIdAdded',
      );
    } else if (collectionLink.href === getSelfLink(stacParent) && stacChild.collection !== stacParent.id) {
      // Links to this collection, but collection ID is wrong
      stacChild.collection = stacParent.id;
      stacChild.properties.datetime = CliDate;
      logger.info({ stacChildFile, collection: stacChild.collection, stacParentFile }, 'STAC:ItemCollectionIdUpdated');
    } else {
      logger.info(
        {
          stacChildFile,
          stacParentFile,
          collectionLink: collectionLink.href,
          childCollectionId: stacChild.collection,
          parentId: stacParent.id,
        },
        'STAC:ItemCollectionLinkAndIdUpToDate',
      );
    }
  }
  const parentLink = stacChild.links.find((link) => link.rel === 'parent');
  if (parentLink === undefined) {
    stacChild.links.push({ rel: 'parent', href: stacParentFile, type: 'application/json' });
    if (childIsItem) {
      stacChild.properties.datetime = CliDate;
    } else {
      stacChild['updated'] = CliDate;
    }
    logger.info({ stacChildFile, stacParentFile }, 'STAC:ParentLinkAdded');
    return stacChild;
  }
  if (parentLink.href !== stacParentFile) {
    logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildHasDifferentParentLink');
    return stacChild;
  }
  logger.info({ stacChildFile, stacParentFile }, 'STAC:ParentLinkAlreadyUpToDate');
  return stacChild;
}

/**
 * Ensure that the STAC parent has a link to the specified STAC child.
 * If the link is missing or incorrect, it will be added or updated.
 * If the child is a STAC Item and the parent is a STAC Collection,
 * the parent's spatial and temporal extents will also be updated to include the child's extents.
 *
 * @param stacParent
 * @param stacChild
 *
 * @returns The updated STAC parent with the correct child link and updated extents (if applicable).
 */
export function addChildDataToParent(
  stacParent: StacCollection | StacCatalog,
  stacChild: StacItem | StacCollection | StacCatalog,
): StacCollection | StacCatalog {
  const stacParentFile = getSelfLink(stacParent);
  const stacChildFile = getSelfLink(stacChild);
  const childIsItem = stacChild.type === 'Feature';
  const parentIsCollection = stacParent.type === 'Collection';
  const expectedRel = childIsItem ? 'item' : 'child';
  const expectedType = childIsItem ? 'application/geo+json' : 'application/json';
  const newLinkStats = createFileStatsFromStac(stacChild);
  const newLinkToChild = {
    rel: expectedRel,
    href: stacChildFile,
    type: expectedType,
    ...newLinkStats,
    title: stacChild.title,
  } as StacLink;

  const oldLinkToChild = stacParent.links.find(
    (link) => link.href === stacChildFile && link.rel === expectedRel && link.type === expectedType,
  );
  if (oldLinkToChild === undefined) {
    stacParent.links.push(newLinkToChild);
    stacParent['updated'] = CliDate;
    if (childIsItem && parentIsCollection) {
      logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildLinkAndExtentsAdded');
      return addExtentFromItemToCollection(stacParent, stacChild);
    }
    logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildLinkAdded');
    return stacParent;
  }
  if (compareStacAssets(newLinkToChild, oldLinkToChild)) {
    logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildLinkAlreadyUpToDate');
    return stacParent;
  }
  stacParent.links = stacParent.links.filter(
    (link) => !(link.href === stacChildFile && link.rel === expectedRel && link.type === expectedType),
  );
  stacParent.links.push(newLinkToChild);
  stacParent['updated'] = CliDate;
  // FIXME: update extents in Collection when updating existing Items?
  //  To do this reliably, we need to read all child items again.
  //  For now, we skip updating extents on link updates.
  logger.info({ stacChildFile, stacParentFile }, 'STAC:ChildLinkUpdated');
  return stacParent;
}

function addExtentFromItemToCollection(stacCollection: StacCollection, stacItem: StacItem): StacCollection {
  if (stacItem.bbox) {
    stacCollection.extent.spatial.bbox.push(stacItem.bbox);
  }
  if (stacItem.properties.start_datetime) {
    stacCollection.extent.temporal.interval.push([
      stacItem.properties.start_datetime,
      stacItem.properties.end_datetime ?? null,
    ]);
  }
  return stacCollection;
}
