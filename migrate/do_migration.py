from migrate import Migrator
from schemas.example import Example


def main():
    migrator = Migrator(Example(),
                        create_devices=True,
                        start_date="2014-09-19T00:00:00Z",
                        pool_size=5)

    migrator.migrate_all_series()


if __name__ == "__main__":
    main()
