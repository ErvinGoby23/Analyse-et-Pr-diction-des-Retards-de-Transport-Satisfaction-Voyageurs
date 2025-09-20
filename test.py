import requests
import xml.etree.ElementTree as ET

url = "https://proxy.transport.data.gouv.fr/resource/sncf-siri-lite-situation-exchange"
response = requests.get(url)

# Parser le XML
root = ET.fromstring(response.content)

# Le XML utilise des "namespaces", il faut les déclarer
ns = {"siri": "http://www.siri.org.uk/siri"}

# Exemple : récupérer tous les titres de perturbation (InfoMessage)
for situation in root.findall(".//siri:PtSituationElement", ns):
    creation_time = situation.find("siri:CreationTime", ns)
    number = situation.find("siri:SituationNumber", ns)
    progress = situation.find("siri:Progress", ns)

    print("---- Incident ----")
    if creation_time is not None:
        print("Créé le :", creation_time.text)
    if number is not None:
        print("Numéro :", number.text)
    if progress is not None:
        print("Progression :", progress.text)
