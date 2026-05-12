# fix-schema.sed
# GNU sed, with -E

/^@jsonSchema\("([^"]+)\.yaml"\)$/{
  s/^@jsonSchema\("([^"]+)\.yaml"\)$/@jsonSchema("\1")/
  h
  b
}

/^@jsonSchema\("([^"]+)"\)$/{
  h
  b
}

/^model[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\{/{
  x
  s/^@jsonSchema\("([^"]+)"\)$/\1/
  x
  G
  s/^model[[:space:]]+[A-Za-z_][A-Za-z0-9_]*([[:space:]]*\{)\n([^\n]+)$/model \2\1/
}
