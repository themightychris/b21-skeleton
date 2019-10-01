# b21-skeleton

Defaults for all B21 and affiliated schools.


## Event Handlers:

#### Export Data:
To setup, run the following command in the console:

First run this command to verify that the cron job script exists, if not it will create it.
```
touch /etc/cron.d/slate-cbl-data-export
```

```
echo "{MINUTE} {HOUR} * * *\troot\techo \"Emergence\EventBus::fireEvent('export-data', 'Slate\\CBL');\" | /usr/bin/emergence-shell {SITE_ID}" | sudo tee -a /etc/cron.d/slate-cbl-data-export
```

Replace the {SITE_ID} with the respective site ID in `site.json`.
Replace the {HOUR} & {MINUTE} with the time you want the script to be ran daily (military time format).

Example:
This will automate the export for the school with an ID of `test-school` at 8:30 PM daily (server time)

```
echo "30 20 * * *\troot\techo \"Emergence\EventBus::fireEvent('export-data', 'Slate\\CBL');\" | /usr/bin/emergence-shell test-school" | sudo tee -a /etc/cron.d/slate-cbl-data-export
```