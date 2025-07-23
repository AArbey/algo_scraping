# Utiliser les services


> Si vous utilisez git LFS pour le stockage des données, assurez-vous que git lfs ne prend pas trop de données sur la machine !
> Si besoin, vous pouvez supprimer le cache de git lfs avec la commande suivante :
> ```bash
> git lfs prune
> ```

## Installation

### Python3 si besoin (Ici sur Ubuntu 24)

```bash
sudo apt install python3 python3-pip
sudo apt install python3.12-venv # Bien modifier la version si nécessaire
```

### Xvfb

```bash
sudo apt install xvfb
```

### Installer google chrome sur ubuntu

```bash
sudo sh -c 'echo "deb [arch=amd64] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list'
wget -O- https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo tee /etc/apt/trusted.gpg.d/linux_signing_key.pub
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 78BD65473CB3BD13
sudo apt-get update

# SI sous ubuntu>=22 :
sudo apt-key export D38B4796 | sudo gpg --dearmour -o /etc/apt/trusted.gpg.d/chrome.gpg

# Puis, installez Google Chrome :
sudo apt-get install google-chrome-stable
```

### Créer un venv

```bash
# Créer un environnement virtuel
python3 -m venv /home/scraping/venv
# Activer l'environnement virtuel
source /home/scraping/venv/bin/activate
# Installer les dépendances
pip install -r ./requirements.txt
```


## Lancer le scraping

### Carrefour

#### Créez un fichier .service dans /etc/systemd/system/ :

```bash
sudo nano /etc/systemd/system/carrefour_scraping.service
```

#### Collez cette configuration :

```bash
[Unit]
Description=Scraping Carrefour Service
After=network.target

[Service]
Type=simple
Environment="PATH=/home/scraping/venv/bin:/usr/bin"
ExecStart=/home/scraping/venv/bin/python /home/scraping/algo_scraping/CARREFOUR/scraping_carrefour.py
WorkingDirectory=/home/scraping/algo_scraping/CARREFOUR
Environment="DISPLAY=:103"
Environment="PYTHONUNBUFFERED=1"
Environment="HOME=/home/scraping"

# Journalisation
SyslogIdentifier=carrefour_scraping
StandardOutput=syslog
StandardError=syslog

# Redémarrage
Restart=on-failure
RestartSec=30

# Exécuter en tant qu'utilisateur via sudo
User=root

[Install]
WantedBy=multi-user.target
```

#### Activer le service

```bash
# Recharger systemd
sudo systemctl daemon-reload

# Activer le démarrage au boot
sudo systemctl enable carrefour_scraping

# Démarrer le service
sudo systemctl start carrefour_scraping
```

#### Afficher les logs 

```bash
# Pour afficher les logs du service Carrefour
sudo journalctl -u carrefour_scraping -f
```

### Leclerc

#### Créez un fichier .service dans /etc/systemd/system/ :

```bash
sudo nano /etc/systemd/system/leclerc_scraping.service
```

#### Collez cette configuration :

```bash
[Unit]
Description=Scraping Leclerc Service
After=network.target

[Service]
Type=simple
Environment="PATH=/home/scraping/venv/bin:/usr/bin"
ExecStart=/home/scraping/venv/bin/python /home/scraping/algo_scraping/LECLERC/LECLERC.py
WorkingDirectory=/home/scraping/algo_scraping/LECLERC
Environment="DISPLAY=:103"
Environment="PYTHONUNBUFFERED=1"
Environment="HOME=/home/scraping"

# Journalisation
SyslogIdentifier=leclerc_scraping
StandardOutput=syslog
StandardError=syslog

# Redémarrage
Restart=on-failure
RestartSec=30

# Exécuter en tant qu'utilisateur via sudo
User=root

[Install]
WantedBy=multi-user.target
```

#### Activer le service

```bash
# Recharger systemd
sudo systemctl daemon-reload

# Activer le démarrage au boot
sudo systemctl enable leclerc_scraping

# Démarrer le service
sudo systemctl start leclerc_scraping
```

#### Afficher les logs 

```bash
# Pour afficher les logs du service Leclerc     
sudo journalctl -u leclerc_scraping -f
```

### Rakuten

#### Créez un fichier .service dans /etc/systemd/system/ :

```bash
sudo nano /etc/systemd/system/rakuten_scraping.service
```

#### Collez cette configuration :

```bash
[Unit]
Description=Scraping Rakuten Service
After=network.target

[Service]
Type=simple
Environment="PATH=/home/scraping/venv/bin:/usr/bin"
ExecStart=/home/scraping/venv/bin/python /home/scraping/algo_scraping/RAKUTEN/RAKUTEN.py
WorkingDirectory=/home/scraping/algo_scraping/RAKUTEN
Environment="DISPLAY=:103"
Environment="PYTHONUNBUFFERED=1"
Environment="HOME=/home/scraping"

# Journalisation
SyslogIdentifier=rakuten_scraping
StandardOutput=syslog
StandardError=syslog

# Redémarrage
Restart=on-failure
RestartSec=30
# Exécuter en tant qu'utilisateur via sudo
User=root

[Install]
WantedBy=multi-user.target
```

#### Activer le service

```bash
# Recharger systemd
sudo systemctl daemon-reload

# Activer le démarrage au boot
sudo systemctl enable rakuten_scraping

# Démarrer le service
sudo systemctl start rakuten_scraping
```

#### Afficher les logs 

```bash
# Pour afficher les logs du service Rakuten
sudo journalctl -u rakuten_scraping -f
```

### Cdiscount

#### Créez un fichier .service dans /etc/systemd/system/ :

```bash
sudo nano /etc/systemd/system/cdiscount_scraping.service
```

#### Collez cette configuration :

```bash
[Unit]
Description=Scraping Cdiscount Service
After=network.target

[Service]
Type=simple
Environment="PATH=/home/scraping/venv/bin:/usr/bin"
ExecStart=/home/scraping/venv/bin/python /home/scraping/algo_scraping/CDISCOUNT/cdiscount_scrap.py
WorkingDirectory=/home/scraping/algo_scraping/CDISCOUNT
Environment="DISPLAY=:103"
Environment="PYTHONUNBUFFERED=1"
Environment="HOME=/home/scraping"
# Journalisation
SyslogIdentifier=cdiscount_scraping
StandardOutput=syslog
StandardError=syslog

# Redémarrage
Restart=on-failure
RestartSec=30
# Exécuter en tant qu'utilisateur via sudo
User=root

[Install]
WantedBy=multi-user.target
```

#### Activer le service

```bash
# Recharger systemd
sudo systemctl daemon-reload

# Activer le démarrage au boot
sudo systemctl enable cdiscount_scraping

# Démarrer le service
sudo systemctl start cdiscount_scraping
```

#### Afficher les logs 

```bash
# Pour afficher les logs du service Cdiscount
sudo journalctl -u cdiscount_scraping -f
```

### Amazon

#### Créez un fichier .service dans /etc/systemd/system/ :

```bash
sudo nano /etc/systemd/system/amazon_scraping.service
```

#### Collez cette configuration :

```bash
[Unit]
Description=Scraping Amazon Service
After=network.target

[Service]
Type=simple
Environment="PATH=/home/scraping/venv/bin:/usr/bin"
ExecStart=/home/scraping/venv/bin/python /home/scraping/algo_scraping/AMAZON/AMAZON.py
WorkingDirectory=/home/scraping/algo_scraping/AMAZON
Environment="DISPLAY=:103"
Environment="PYTHONUNBUFFERED=1"
Environment="HOME=/home/scraping"
# Journalisation
SyslogIdentifier=amazon_scraping
StandardOutput=syslog
StandardError=syslog

# Redémarrage
Restart=on-failure
RestartSec=30
# Exécuter en tant qu'utilisateur via sudo
User=root

[Install]
WantedBy=multi-user.target
```

#### Activer le service

```bash
# Recharger systemd
sudo systemctl daemon-reload

# Activer le démarrage au boot
sudo systemctl enable amazon_scraping

# Démarrer le service
sudo systemctl start amazon_scraping
```

#### Afficher les logs 

```bash
# Pour afficher les logs du service Amazon
sudo journalctl -u amazon_scraping -f
```

### La FNAC

#### Créez un fichier .service dans /etc/systemd/system/ :

```bash
sudo nano /etc/systemd/system/fnac_scraping.service
```

#### Collez cette configuration :

```bash
[Unit]
Description=Scraping FNAC Service
After=network.target
[Service]
Type=simple
Environment="PATH=/home/scraping/venv/bin:/usr/bin"
ExecStart=/home/scraping/venv/bin/python /home/scraping/algo_scraping/FNAC/FNAC.py
WorkingDirectory=/home/scraping/algo_scraping/FNAC
Environment="HOME=/home/scraping"

# Journalisation
SyslogIdentifier=fnac_scraping
StandardOutput=syslog
StandardError=syslog

# Redémarrage
Restart=on-failure
RestartSec=30

# Exécuter en tant qu'utilisateur via sudo
User=root
[Install]
WantedBy=multi-user.target
```

#### Activer le service

```bash
# Recharger systemd
sudo systemctl daemon-reload

# Activer le démarrage au boot
sudo systemctl enable fnac_scraping

# Démarrer le service
sudo systemctl start fnac_scraping
```

#### Afficher les logs 

```bash
# Pour afficher les logs du service FNAC
sudo journalctl -u fnac_scraping -f
```

## Lancer tout les services de scraping

```bash
# Pour démarrer tous les services de scraping
sudo systemctl start fnac_scraping
sudo systemctl start amazon_scraping
sudo systemctl start cdiscount_scraping 
sudo systemctl start rakuten_scraping
sudo systemctl start leclerc_scraping
sudo systemctl start carrefour_scraping
```

## Couper les services de scraping

```bash
# Pour arrêter le service FNAC
sudo systemctl stop fnac_scraping
sudo systemctl stop amazon_scraping
sudo systemctl stop cdiscount_scraping
sudo systemctl stop rakuten_scraping
sudo systemctl stop leclerc_scraping
sudo systemctl stop carrefour_scraping
```

## Lancer la visualisation 

### Manuellement

```bash
nohup python3 /home/scraping/algo_scraping/visualiser/visualise_data_leclerc.py > /home/scraping/algo_scraping/visualiser/output_leclerc.log 2>&1 &
nohup python3 /home/scraping/algo_scraping/visualiser/visualise_data_cdiscount.py > /home/scraping/algo_scraping/visualiser/output_cdiscount.log 2>&1 &
nohup python3 /home/scraping/algo_scraping/visualiser/visualise_data_rakuten.py > /home/scraping/algo_scraping/visualiser/output_rakuten.log 2>&1 &
nohup python3 /home/scraping/algo_scraping/visualiser/visualise_data_carrefour.py > /home/scraping/algo_scraping/visualiser/output_carrefour.log 2>&1 &
nohup python3 /home/scraping/algo_scraping/visualiser/visualise_data_amazon.py > /home/scraping/algo_scraping/visualiser/output_amazon.log 2>&1 &
```

### Avec des services systemd

On peut transformer chaque script en service systemd pour qu’il redémarre automatiquement en cas d’erreur. 


#### Pour `visualise_data_leclerc.py` :

```bash
# Créer le fichier /etc/systemd/system/visualise_leclerc.service
cat <<EOF | sudo tee /etc/systemd/system/visualise_leclerc.service
[Unit]
Description=Visualisation Leclerc - suivi des prix - Port 8050
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=/home/scraping/algo_scraping/visualiser
ExecStart=/usr/bin/python3 /home/scraping/algo_scraping/visualiser/visualise_data_leclerc.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Recharger systemd, activer et démarrer le service
sudo systemctl daemon-reload
sudo systemctl enable visualise_leclerc.service
sudo systemctl start visualise_leclerc.service

# Vérifier le statut
sudo systemctl status visualise_leclerc.service
```


#### Pour `visualise_data_cdiscount.py` :

```bash
# Créer le fichier /etc/systemd/system/visualise_cdiscount.service
cat <<EOF | sudo tee /etc/systemd/system/visualise_cdiscount.service
[Unit]
Description=Visualisation Cdiscount - suivi des prix - Port 8054
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=/home/scraping/algo_scraping/visualiser
ExecStart=/usr/bin/python3 /home/scraping/algo_scraping/visualiser/visualise_data_cdiscount.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Recharger systemd, activer et démarrer le service
sudo systemctl daemon-reload
sudo systemctl enable visualise_cdiscount.service
sudo systemctl start visualise_cdiscount.service

# Vérifier le statut
sudo systemctl status visualise_cdiscount.service
```


#### Pour `visualise_data_rakuten.py` :

```bash
# Créer le fichier /etc/systemd/system/visualise_rakuten.service
cat <<EOF | sudo tee /etc/systemd/system/visualise_rakuten.service
[Unit]
Description=Visualisation Rakuten - suivi des prix - Port 8052
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=/home/scraping/algo_scraping/visualiser
ExecStart=/usr/bin/python3 /home/scraping/algo_scraping/visualiser/visualise_data_rakuten.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable visualise_rakuten.service
sudo systemctl start visualise_rakuten.service
sudo systemctl status visualise_rakuten.service
```


#### Pour `visualise_data_carrefour.py` :

```bash
# Créer le fichier /etc/systemd/system/visualise_carrefour.service
cat <<EOF | sudo tee /etc/systemd/system/visualise_carrefour.service
[Unit]
Description=Visualisation Carrefour - suivi des prix - Port 8051
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=/home/scraping/algo_scraping/visualiser
ExecStart=/usr/bin/python3 /home/scraping/algo_scraping/visualiser/visualise_data_carrefour.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable visualise_carrefour.service
sudo systemctl start visualise_carrefour.service
sudo systemctl status visualise_carrefour.service
```


#### Pour `visualise_data_amazon.py` :

```bash
# Créer le fichier /etc/systemd/system/visualise_amazon.service
cat <<EOF | sudo tee /etc/systemd/system/visualise_amazon.service
[Unit]
Description=Visualisation Amazon - suivi des prix - Port 8053
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=/home/scraping/algo_scraping/visualiser
ExecStart=/usr/bin/python3 /home/scraping/algo_scraping/visualiser/visualise_data_amazon.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable visualise_amazon.service
sudo systemctl start visualise_amazon.service
sudo systemctl status visualise_amazon.service
```


## Autres

### Les scripts add_batch_id.py

Ils permettent d'ajouter un identifiant de lot à chaque ligne d'un fichier CSV, qui identifie les batch de produits analysés. Ils identifient donc chaque passage de scraping. Ces scripts sont uniquement utilisés pour adapter des fichiers CSV qui n'utilisaient pas encore cette fonctionnalité.

## Le monitoring 

### Créer le fichier de service

```bash
sudo nano /etc/systemd/system/monitoring.service
```

### Collez cette configuration :

```bash
[Unit]
Description=Monitoring Service
After=network.target

[Service]
Type=simple
User=scraping
Group=scraping
WorkingDirectory=/home/scraping/algo_scraping/utilitaires
Environment="PATH=/home/scraping/venv/bin:/usr/bin"
# Add your bot credentials here
Environment="DISCORD_BOT_TOKEN={YOUR BOT TOKEN}"
Environment="DISCORD_CHANNEL_ID={YOUR CHANNEL ID}"
ExecStart=/home/scraping/venv/bin/python /home/scraping/algo_scraping/utilitaires/monitor.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target

```
### Activer le service

```bash
# Recharger systemd
sudo systemctl daemon-reload    

# Activer le démarrage au boot
sudo systemctl enable monitoring.service

# Démarrer le service
sudo systemctl start monitoring.service
```
