Set-Location .\docker
docker build -t odbcsv-test .

docker run --rm `
   --publish 1433:1433 --name sql1 `
   --detach odbcsv-test
Set-Location ..