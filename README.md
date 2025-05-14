# TELECOM Data Analyzer

Cette application Flask permet d'analyser des exports de données réseaux télécoms, les stocker dans une base PostgreSQL, et en faire des vérifications de cohérence.

---

## ⚙️ Installation

1. **Clonez le dépôt** :

   ```bash
   git clone https://github.com/SeynabouS/-telecom-data-analyzer.git
   cd -telecom-data-analyzer
   ```

2. **Créez et configurez votre environnement `.env`** :
   Copiez `.env.example` et remplissez vos informations de connexion :

   ```bash
   cp .env.example .env
   ```

3. **Installez les dépendances** :

   ```bash
   pip install -r requirements.txt
   ```

4. **Créez la base de données PostgreSQL** :

   Dans PostgreSQL :

   ```sql
   CREATE DATABASE TELECOM;
   CREATE SCHEMA gracethd;
   ```

5. **Créez la table `exports`** :
   Exécutez le contenu du fichier :

   ```
   creation table exports.txt
   ```

---

## 👤 Création d'un utilisateur administrateur

Utilisez le script Python `create_user.py` pour créer un utilisateur :

```bash
python create_user.py <username> <password>
```

Par exemple :

```bash
python create_user.py admin admin123
```

Ce script :

* Vérifie si l'utilisateur existe
* Hash le mot de passe
* Ajoute l'utilisateur à la base

---

## 🚀 Lancer l'application

```bash
flask run
```

L'application sera accessible à l'adresse :
[http://127.0.0.1:5000](http://127.0.0.1:5000)

---

## 📂 Structure du projet

* `app/` : Contient le code principal de l'application
* `uploads/` : Dossier contenant les fichiers d'exports à analyser
* `.env.example` : Modèle de configuration
* `requirements.txt` : Librairies Python nécessaires
* `create_user.py` : Script pour créer un utilisateur
* `creation table exports.txt` : Requête SQL pour la table `exports`

---

