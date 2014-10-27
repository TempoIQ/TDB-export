from migrate import Migrator
from customers.root3 import Root3 as Scheme


def main():
    migrator = Migrator(Scheme(),
                        create_devices=True,
                        write_data=False,
                        start_date="2014-09-27T00:00:00Z",
                        pool_size=10)

    migrator.migrate_all_series()


if __name__ == "__main__":
    main()
