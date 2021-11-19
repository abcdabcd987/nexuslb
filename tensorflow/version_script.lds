NEXUS_1.0 {
  # Export nexus namespace
  global:
    extern "C++" {
      nexus::*;
    };

  # Hide everything else.
  local:
    *;
};
