{ #pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/17.09.tar.gz") {}
  pkgs ? import <nixpkgs> {}
}:
pkgs.buildGoPackage rec {
  name = "batbroker-${version}";
  version = "dev";

  goPackagePath = "gitlab.inria.fr/batsim/batbroker";
  #subPackages = [ "message" ];

  src = ./.;

  goDeps = ./deps.nix;

  LC_ALL = "C";

  buildInputs = with pkgs; [
    pkgconfig
    go
    zeromq
  ];
}
