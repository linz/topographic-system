import "@typespec/versioning";

using TypeSpec.Versioning;

@versioned(Versions)
namespace Topography;

enum Versions {
  v0_3: "0.3",
  v0_4: "0.4",
}
