import os

# grabs the folder where the script runs
basedir = os.path.abspath(os.path.dirname(__file__))
BASEDIR = basedir

# enable debug mode
DEBUG = False

# secret key for session management
#   put the key in secret-key.txt and don't add that file to the repository
#   if others might have access to the repository, e.g. on github
SECRET_KEY = file(os.path.join(basedir,'secret-key.txt')).read().strip()

# connect to the database
SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'database.db')
SQLALCHEMY_MIGRATE_REPO = os.path.join(basedir, 'db_repository')
