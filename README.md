# Criminal Stats Dashboard for Mexico 🚔📊

Welcome to my project's README! This is where you get a sneak peek into Mexico crime data. 🦸‍♂️🇲🇽

<p align="center">
    <img src="./assets/dashboard/dashboard_mex_map.png" alt="Crimes Per Geographical Region" width=60% height=60%/>
</p>

## What's This All About? 🤔

The goal? Simple. We're building a dashboard to showcase criminal stats in Mexico. It's a blend of cool tech, data magic, and a sprinkle of detective work. 🔍💻

## Tech Stack 🛠️

- **Google Cloud Platform (GCP):**
  - Dataproc: Sparkin' up those data jobs.
  - Google Cloud Storage: Where our data calls home.
  - BigQuery: Our data warehouse chill spot.
  - Compute Engine: Hosting a VM for our shenanigans.

- **Apache Airflow:** The conductor orchestrating our data symphony.

- **DBT (Data Build Tool):** Crafting our data models with finesse. 🔨

- **Docker:** Wrapping Airflow, DBT, and friends in neat little containers. 🐳

- **Terraform:** Building castles in the cloud (GCP). 🏰

## How Does It All Fit Together? 🧩

Imagine a world where data flows like a river, getting processed, stored, and visualized seamlessly. That's our game plan! 🌊📈

![Architecture Diagram](./assets/architecture/dez_arch_emi_v0.3.drawio.png)

## Data Flow Overview 🌊

Our data journey is like riding a wave 🏄‍♂️—it starts with a splash and ends with a splashier insight! Here's the lowdown:

1. **Data Ingestion 🚀:**
   - Raw data rides in from all corners of Mexico, packing juicy details about crimes, locations, and times. Think of it as our data surfers catching the gnarliest waves of info.
   - We scoop up this data and stash it in our beachfront hangout: Google Cloud Storage (GCS). It's like the cool surf shack where all the rad data hangs out.

2. **Data Processing 🌊:**
   - Time to ride the wave! With Apache Airflow as our wave master, we shred through tasks like data cleanup, transformation, and enrichment. 🤙
   - Spark jobs on Google Dataproc clusters do the heavy lifting, turning raw data into polished pearls ready for analysis.

3. **Data Storage 🏄‍♀️:**
   - Our polished pearls make their way to the BigQuery beach house, where they chillax in structured tables. BigQuery is our go-to spot for sippin' on SQL queries and soaking up insights.
   - Sometimes, we stash intermediate goodies here too, for quick access during our surfing sessions.

4. **Data Modeling 💪:**
   - Now it's time to sculpt! With DBT (Data Build Tool), we mold our data into sleek, well-defined shapes. Think of it as giving our insights a killer beach bod.
   - SQL magic in DBT transforms our data, adding layers of meaning and depth, turning raw numbers into stories worth telling.

5. **Data Visualization 🌊:**
   - Hang on tight, 'cause here come the waves of insight! 🌊 We hop on LookerStudio visualization tool to craft custom dashboards to ride those data waves.
   - Interactive dashboards make it a breeze for everyone to catch a glimpse of the big kahuna: key metrics, trends, and patterns in Mexico's crime scene.

And that's how we roll—riding the data waves, catching insights, and turning them into action-packed adventures in the world of Mexican crime stats! 🔍

![DataLake & DataWarehouse](./assets/data/DataLake_&_DataWarehouse.drawio.png)

## DataWarehouse Overview

![DataWarehouse](./assets/entity_realtionship/fact_diagram.png)

![DataWarehouse](./assets/entity_realtionship/dbt_liniage.png)

## Getting Started 🚀

1. **Clone Me:**
   ```bash
   git clone https://github.com/emilianolel/dez-project-emi.git
   ```

2. **Get Things Ready:**
   ```bash
    cd dez-project-emi
   ```
   # Set up your environment (you got this!).

3. **Hook Up GCP:**
   - [Spin up a GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects).
   - Flip those API switches (Dataproc, BigQuery, LookerStudio).
   - Make some service accounts dance to your tune. Here is an example of the roles you will need
   <p align="center">
      <img src="./assets/service_account_roles.png" alt="Service Account_roles" width=30% height=30%/>
   </p>
   - Create a Key for that service account and save it.

4. **Fire Up Terraform:**
   Open `terraform/variables.tf` and replace this variables for the ones that fits you better such as your service account and secret `.json` file.
   ```bash
   cd terraform
   ```
   ```bash
   terraform init
   ```
   ```bash
   terraform apply
   ```

5. **Connect to VM**
   - Open Google Cloud Console and get the `external ip` of your VM.
   - Connect to it using:
   ```bash
   ssh -i <path/to/ssh/private/key> <username>@<external_ip_address>
   ```
      - If got the following error message:
      ```
      @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
      @    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @
      @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
      IT IS POSSIBLE THAT SOMEONE IS DOING SOMETHING NASTY!
      Someone could be eavesdropping on you right now (man-in-the-middle attack)!
      It is also possible that a host key has just been changed.
      The fingerprint for the ED25519 key sent by the remote host is
      ```
      you can run:
      ```bash
      ssh-keygen -R <external_ip_address>
      ```

6. **Install Docker:**
    - From the project directory simply run 
    ```bash
   ./setup/install_docker.sh 
    ```
    - Then run the following commands:
    ```bash
    sudo groupadd docker
    ```
    ```bash
    sudo usermod -aG docker $USER
    ```
    ```bash
    newgrp docker
    ```
    ```bash
    docker run hello-world
    ```

7. **Set up Airflow ENV Variables**
   - Open the `airflow/airflow.env` and modify the following variables:
   ```bash
   # Params to touch
   USER_GCP_SECRET_PATH=.secrets/gcp/
   USER_GCP_SECRET_NAME=<your-secret-json>
   GCP_SERVICE_ACCOUNT=<your-service-account-id>@<your-gcp-project>.iam.gserviceaccount.com
   GCP_PROJECT=<your-gcp-project>
   ```
   - Run the following command in order to create the Dockerfile:
   ```bash
   ./docker/make_dockerfile.sh
   ```

5. **Launch Airflow:**
   - Dockerize Airflow. Change the following line of the file `./airflow/scripts/entrypoint.sh`
   ```bash
   airflow db init
   ```
   to
   ```bash
   airflow db reset
   ```
   - Blast off using Docker Compose or Kubernetes.

## Let's Roll! 🎲

1. **Start Airflow:**
   # Get those Airflow gears turning.
   ![DAGs](./assets/airflow/dags.png)

2. **Check Out the UI:**
   - Visit `http://localhost:8080`.
   - Dive into the DAGs and hit play!

3. **Explore the Dashboard:**
   - Access our crime-busting dashboard via the provided link.
   [![Crimes Per Geographical Region](./assets/dashboard/dashboard_geo.png)](https://lookerstudio.google.com/reporting/e4caefba-78d3-4d88-8dd0-b53e55b467c9/page/p_svlrf4mvfd)

## Wanna Join the Adventure? 🦸‍♀️

We're all about teamwork! If you're itching to hop on board, here's how:

1. Fork this repo.
2. Cook up your feature branch (`git checkout -b feature/YourFeature`).
3. Add your magic (`git commit -am 'Added awesome stuff'`).
4. Push it like you mean it (`git push origin feature/YourFeature`).
5. Open that sweet, sweet Pull Request.

## Legal Stuff 📜

This project is licensed under the [MIT License](./LICENCE). Go ahead, have fun with it!

---

