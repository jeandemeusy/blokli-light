# bloklid.nix - BLOKLID daemon package definitions
#
# Defines all variants of the BLOKLID daemon for different platforms and profiles.
# BLOKLID is the companion indexer for the HOPR network.

{
  lib,
  pkgs,
  builders,
  sources,
  bloklidCrateInfo,
  buildVersion,
  rev,
  buildPlatform,
  nixLib,
}:

let
  mkbloklidBuildArgs =
    { src, depsSrc }:
    {
      inherit
        src
        depsSrc
        rev
        buildVersion
        ;
      cargoExtraArgs = "--bins"; # Build all binary targets
      cargoToml = ./../../bloklid/Cargo.toml;
    };

  # Production builds includes bloklid in each platform derivation.
  # prependPackageName = false lets us specify both packages explicitly via cargoExtraArgs.
  mkBloklidPlatformPackages =
    platform:
    let
      args =
        (mkbloklidBuildArgs {
          src = sources.main;
          depsSrc = sources.deps;
        })
        // {
          prependPackageName = false;
          cargoExtraArgs = "-p blokli-light --bin blokli-light --locked";
        };
      name = "binary-bloklid-${platform}";
    in
    {
      "${name}" = builders.${platform}.callPackage nixLib.mkRustPackage args;
    }
    // lib.optionalAttrs (lib.hasSuffix "-linux" platform) {
      "${name}-dev" = builders.${platform}.callPackage nixLib.mkRustPackage (
        args // { CARGO_PROFILE = "dev"; }
      );
    };

  bloklidPackages = builtins.foldl' (a: b: a // b) { } (
    map mkBloklidPlatformPackages [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ]
  );

in
{
  # Development builds - for local testing and debugging
  bloklid = builders.local.callPackage nixLib.mkRustPackage (mkbloklidBuildArgs {
    src = sources.main;
    depsSrc = sources.deps;
  });

  bloklid-dev = builders.local.callPackage nixLib.mkRustPackage (
    (mkbloklidBuildArgs {
      src = sources.main;
      depsSrc = sources.deps;
    })
    // {
      CARGO_PROFILE = "dev";
    }
  );
}
// bloklidPackages
// {
  # Test and quality assurance builds
  bloklid-test = builders.local.callPackage nixLib.mkRustPackage (
    (mkbloklidBuildArgs {
      src = sources.test;
      depsSrc = sources.deps;
    })
    // {
      runTests = true;
    }
  );

  bloklid-clippy = builders.local.callPackage nixLib.mkRustPackage (
    (mkbloklidBuildArgs {
      src = sources.main;
      depsSrc = sources.deps;
    })
    // {
      runClippy = true; # Run Clippy linter
      prependPackageName = false;
      cargoExtraArgs = "--workspace --all-features";
    }
  );
}
