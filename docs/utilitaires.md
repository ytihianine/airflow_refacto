## Commandes utilitaires

## Config git
```
git config --global credential.helper 'cache --timeout=360000'
```

### Export la structure des tables
On utilisera [dbdiagram](https://dbdiagram.io)
```
npm install -g @dbml/cli
db2dbml postgres 'postgresql://user:password@localhost:5432/dbname?schemas=schema1,schema2,schema3' -o database.dbml
```
