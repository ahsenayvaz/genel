# CAS RN® – Chemical Abstracts Service Registry Number®

## Was ist CAS?

CAS (Chemical Abstracts Service) ist eine Abteilung der American Chemical Society. Sie betreibt unter anderem **Chemical Abstracts (CA)**, einen Index, der Daten zu chemischen Strukturen aus wissenschaftlichen Publikationen zusammenführt, sowie mehrere kommerzielle Produkte (SciFinder, CAS Patents, CAS References, CAS Registry® u. a.).

## CAS Registry®

Die CAS Registry® ist eine Datenbank mit über 290 Millionen offengelegten chemischen Substanzen, kuratiert aus wissenschaftlicher Literatur und weiteren Quellen. Sie wird laufend um neue Substanzen und aktuelle Informationen erweitert.

## CAS Registry Number® (CAS RN®)

Eigenschaften:

- eindeutiger, unverwechselbarer Bezeichner für eine bestimmte Substanz
- verknüpft alle verfügbaren Daten und Forschungsergebnisse zu dieser Substanz
- leicht validierbar (Prüfziffer)
- besteht aus bis zu 10 Ziffern, durch Bindestriche in drei Teile gegliedert
- **keine inhaltliche Bedeutung** – die Nummern werden fortlaufend vergeben

### Aufbau

`XX(XXXXX)-XX-C`

| Teil   | Länge       | Bedeutung                                                                                             |
|--------|-------------|-------------------------------------------------------------------------------------------------------|
| Teil 1 | 2–7 Ziffern | fortlaufende Nummer                                                                                   |
| Teil 2 | 2 Ziffern   | fortlaufende Nummer                                                                                   |
| Teil 3 | 1 Ziffer    | [Prüfziffer zur Validierung](https://www.cas.org/training/documentation/chemical-substances/checkdig) |

### Beispiele

- Koffein: `58-08-2` ([Nachschlagen](https://commonchemistry.cas.org/detail?cas_rn=58-08-2&search=caffeine) in CAS Common Chemistry)
- Diamant: `7782-40-3` ([Nachschlagen](https://commonchemistry.cas.org/detail?cas_rn=7782-40-3&search=diamond) in CAS Common Chemistry)

## Eine CAS RN® finden

- **[CAS Common Chemistry](https://commonchemistry.cas.org/)**: offen zugängliche Ressource mit knapp 500.000 chemischen Substanzen aus der CAS Registry – für gängige und häufig regulierte Chemikalien
- CAS Registry Lookup Service (Bestellformular): für speziellere Substanzen, Ergebnis innerhalb von 24 Stunden

## Quellen & Links

- [CAS Registry®](https://www.cas.org/cas-data/cas-registry) – offizielle CAS-Website
- [CAS RN® auf Wikipedia (en)](https://en.wikipedia.org/wiki/CAS_Registry_Number)

---

## Kürzel aus den `_source`-Spalten

Übersicht der Quellen-Kürzel, die in `cas_source` und `ocas_source` verwendet werden:

| Kürzel            | Ausgeschrieben                                                                              | Was es ist                                                                                                                           | Link                                                                     |
|-------------------|---------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| **NCBI LactMed**  | National Center for Biotechnology Information – Lactation Medications Database              | US-Datenbank zu Arzneistoffen und Stillzeit, enthält aber auch allgemeine Substanzdaten inkl. CAS                                    | https://www.ncbi.nlm.nih.gov/books/NBK501922/                            |
| **ECHA CHEM**     | European Chemicals Agency – Chemical Database                                               | EU-Chemikalienbehörde, Datenbank zu registrierten Stoffen (REACH/CLP)                                                                | https://echa.europa.eu/de/information-on-chemicals                       |
| **PubChem CID**   | PubChem Compound ID                                                                         | NIH/NCBI-Datenbank für definierte chemische Verbindungen (CID = eindeutige Substanz-ID)                                              | https://pubchem.ncbi.nlm.nih.gov/compound/{CID}                          |
| **PubChem SID**   | PubChem Substance ID                                                                        | wie CID, aber für "Substanzen" wie Extrakte/Gemische ohne einzelne definierte Verbindung (daher bei pflanzlichen Extrakten relevant) | https://pubchem.ncbi.nlm.nih.gov/substance/{SID}                         |
| **NCATS GSRS**    | National Center for Advancing Translational Sciences – Global Substance Registration System | US-Register für pharmazeutische Substanzen inkl. Biologika (UNII-Codes)                                                              | https://gsrs.ncats.nih.gov/                                              |
| **EPA SRS**       | US Environmental Protection Agency – Substance Registry Services                            | US-Umweltbehörde, Substanzregister (oft für Mikroorganismen/Chemikalien mit Umweltbezug)                                             | https://sor.epa.gov/sor_internet/registry/substreg/home/overview/home.do |
| **GDCh**          | Gesellschaft Deutscher Chemiker                                                             | deutsche Fachgesellschaft, teils Quelle für Stoffdaten/Nomenklatur                                                                   | https://www.gdch.de/                                                     |
| **CosIng**        | Cosmetic Ingredient Database (EU-Kommission)                                                | EU-Datenbank für Kosmetikinhaltsstoffe, oft mit CAS für pflanzliche Extrakte                                                         | https://ec.europa.eu/growth/tools-databases/cosing/                      |
| **EMA**           | European Medicines Agency                                                                   | EU-Arzneimittelbehörde, z. B. für Zulassungsdokumente/Wirkstoffbezeichnungen                                                         | https://www.ema.europa.eu/                                               |
| **Sigma-Aldrich** | Sigma-Aldrich (Merck)                                                                       | kommerzieller Chemikalienhändler, oft zuverlässige CAS-Quelle für Referenzsubstanzen                                                 | https://www.sigmaaldrich.com/                                            |

### Einordnung der Verlässlichkeit

- **Behörden-/Institutsdatenbanken** (NCBI/PubChem, ECHA, NCATS, EPA): offizielle, öffentliche Quellen – gut belastbar
- **Sekundärquellen** (Sigma-Aldrich, CosIng, GDCh): Hersteller- bzw. Fachgesellschaftsangaben – in der Praxis üblich und meist zuverlässig, da CAS-Nummern selten fehlerhaft weitergegeben werden, aber nicht behördlich verifiziert