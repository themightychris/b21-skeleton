# b21-skeleton

Defaults for all B21 and affiliated schools.


## Event Handlers:

#### Export Data:
To setup, run the following command in the console:

```
printf "10 23 \t* * *\troot\techo /usr/local/bin/emergence-fire-event {site_id} export-data Slate\\CBL > /dev/null\n" | sudo tee /etc/cron.d/slate-cbl-data-export
```

Replace the {site_id} with the respective