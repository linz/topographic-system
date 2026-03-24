export const StacUpsert = {
    async collections(root: URL, collections: URL[], commit: boolean): Promise<void> {

        console.log('upsert', root.href, String(collections.join('\n')), commit)

    }
}