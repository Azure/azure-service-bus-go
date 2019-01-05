
function Provision-SBInfrastructure {
    terraform init
    terraform apply -auto-approve
    $output = terraform output
    $fullPath = [IO.Path]::Combine($pwd, "./.env")
    [System.IO.File]::WriteAllLines($fullPath, $output)
}

function Deprovision-SBInfrastructure {
    terraform destroy -auto-approve
}

function Build-SBGo {
    go fmt ./... > $null
    go vet ./...
    go build all
}

function Test-SBGo {
    Build-SBGo
    go test -timeout 1100s -v
}

Export-ModuleMember -Function Provision-SBInfrastructure
Export-ModuleMember -Function Deprovision-SBInfrastructure
Export-ModuleMember -Function Build-SBGo
Export-ModuleMember -Function Test-SBGo