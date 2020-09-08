# tap-workday

---

Expected settings

- `username` - The username to use when connecting to Workday.
- `password` - The password to use when connecting to Workday.
- `reports` - An encoded json string which defines the report or reports to extract:
    ```json
    [
      {
        "report_name": "MyReport",
        "report_url": "https://...",
      }
    ]
    ```


---

Copyright &copy; 2019 Stitch
