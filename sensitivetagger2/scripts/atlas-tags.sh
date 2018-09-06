
#!/bin/bash
tag=$1
attr=$2
echo "the input arg is $tag"
curl -u admin:admin http://ip:21000/api/atlas/v2/types/typedefs -X POST -H 'Content-Type: application/json' --data-binary '{"classificationDefs":[{"name":"'$tag'","description":"test","superTypes":[],"attributeDefs":[{"name":"Source","typeName":"string","isOptional":true,"cardinality":"SINGLE","valuesMinCount":0,"valuesMaxCount":1,"isUnique":false,"isIndexable":false},{"name":"Location","typeName":"string","isOptional":true,"cardinality":"SINGLE","valuesMinCount":0,"valuesMaxCount":1,"isUnique":false,"isIndexable":false,"defaultValue":"'$attr'"}]}],"entityDefs":[],"enumDefs":[],"structDefs":[]}'

echo "Created the tag and now assign to right entity $2"

curl -u admin:admin http://ip:21000/api/atlas/entities/5db90d32-9d2f-4668-8a41-6945713591d7/traits -X POST -H 'Content-Type: application/json' --data-binary '{"jsonClass":"org.apache.atlas.typesystem.json.InstanceSerialization$_Struct","typeName":"'$tag'","values":{"Source": "S3_bucket","Location":"'$attr'"}}'

