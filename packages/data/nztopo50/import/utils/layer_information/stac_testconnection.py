
import pystac


data_catalog = "https://d1jzh93b1t1cv.cloudfront.net/data/building/catalog.json"


def test_stac_connection(catalog_url):
	"""Read and validate a STAC catalog, returning its basic metadata."""
	catalog = pystac.read_file(catalog_url)
	catalog.validate()

	collections = list(catalog.get_collections())
	item_count = sum(1 for collection in collections for _ in collection.get_items())
	return {
		"id": catalog.id,
		"title": catalog.title,
		"collections": len(collections),
		"items": item_count,
	}


if __name__ == "__main__":
	details = test_stac_connection(data_catalog)
	print(f"Connected to STAC catalog: {data_catalog}")
	print(f"ID: {details['id']}")
	print(f"Title: {details['title'] or '<none>'}")
	print(f"Collections: {details['collections']}")
	print(f"Items: {details['items']}")

