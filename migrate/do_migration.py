from migrate import Migrator
from customers.pagerduty import PagerDuty as Scheme


def main():
    migrator = Migrator(Scheme(),
                        create_devices=True,
                        write_data=True,
                        start_date="2014-10-20T00:00:00Z",
                        pool_size=15)

    migrator.migrate_all_series()


if __name__ == "__main__":
    main()
