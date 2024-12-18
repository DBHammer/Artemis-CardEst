<div align="left">
  <h1>
    <img src="./resources/logo.png" width=100>
  	Artemis
  </h1>
</div>

> The code source and extended paper version in Artemis

## Getting Started

### Requirements:

- Java 11 or above
- [Maven](https://maven.apache.org/) (`sudo apt install maven` on Ubuntu)
- The DBMS that you want to test

### Step1: Configure schema and data

See src/main/resources/config/apConfig.xml

### Step2: Configure Query

See src/main/resources/config/apConfig.xml

### Step3: Generation

Run src/main/java/ecnu/dbhammer/main/Main.java

## Tested DBMS

|    DBMS    | Status |    Description     |
| :--------: | :----: | :----------------: |
|   MySQL    |  Done  | All types of Joins |
| PostgreSQL |  Done  | All types of Joins |
|    TiDB    |  Done  | All types of Joins |
| OceanBase  |  Done  | All types of Joins |
|  PolarDB   |  TODO  | Have not yet done  |
|   TDSQL    |  TODO  | Have not yet done  |

## Acknowledge

We generate a logo picture from GPT, which is an fabulous artist.

Lastly, thanks to all engineers who help us analyze Cardinality Estmiation issues  :D

## Star History

<a href="https://star-history.com/#DBHammer/Artemis-CardEst&Date">

  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=DBHammer/Artemis-CardEst&type=Date&theme=dark" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=DBHammer/Artemis-CardEst&type=Date" />
    <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=DBHammer/Artemis-CardEst&type=Date" />
  </picture>

</a>
