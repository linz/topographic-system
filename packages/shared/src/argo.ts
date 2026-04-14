export function isArgo(): boolean {
  return process.env['ARGO_NODE_ID'] != null;
}
