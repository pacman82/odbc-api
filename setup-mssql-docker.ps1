Set-Location .\docker
docker build -t odbc-api-test .

docker run --rm `
   --publish 1433:1433 --name sql1 `
   --detach odbc-api-test
Set-Location ..