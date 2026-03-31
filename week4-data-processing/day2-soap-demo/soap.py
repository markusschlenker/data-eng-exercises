from zeep import Client

# Working, active SOAP WSDL
wsdl = "https://www.dataaccess.com/webservicesserver/NumberConversion.wso?WSDL"


# Create client
client = Client(wsdl)

# Call SOAP method
result = client.service.NumberToWords(123)

print("123 →", result)
