pub use sea_orm_migration::prelude::*;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![Box::new(InitialMigration)]
    }
}

pub struct InitialMigration;

impl MigrationName for InitialMigration {
    fn name(&self) -> &str {
        "m20260403_000001_initial"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for InitialMigration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(DailySalts::Table)
                    .if_not_exists()
                    .col(date(DailySalts::Day).primary_key())
                    .col(string(DailySalts::Value).not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Visits::Table)
                    .if_not_exists()
                    .col(pk_auto(Visits::Id))
                    .col(string(Visits::Domain).not_null())
                    .col(string(Visits::VisitorId).not_null())
                    .col(text(Visits::PageUrl).not_null())
                    .col(string(Visits::PageUrlPath).not_null())
                    .col(text_null(Visits::Referrer))
                    .col(string(Visits::Source).not_null())
                    .col(string(Visits::Browser).not_null())
                    .col(string(Visits::OperatingSystem).not_null())
                    .col(string(Visits::DeviceType).not_null())
                    .col(string(Visits::Country).not_null())
                    .col(string(Visits::Region).not_null())
                    .col(string(Visits::City).not_null())
                    .col(string_null(Visits::UtmSource))
                    .col(string_null(Visits::UtmMedium))
                    .col(string_null(Visits::UtmCampaign))
                    .col(string_null(Visits::UtmContent))
                    .col(string_null(Visits::UtmTerm))
                    .col(timestamp_with_time_zone(Visits::StartedAt).not_null())
                    .col(timestamp_with_time_zone(Visits::LastActivityAt).not_null())
                    .col(text_null(Visits::LastVisitedUrl))
                    .col(string_null(Visits::LastVisitedUrlPath))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_visits_domain_started_at")
                    .table(Visits::Table)
                    .col(Visits::Domain)
                    .col(Visits::StartedAt)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_visits_domain_last_activity")
                    .table(Visits::Table)
                    .col(Visits::Domain)
                    .col(Visits::LastActivityAt)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_visits_domain_visitor")
                    .table(Visits::Table)
                    .col(Visits::Domain)
                    .col(Visits::VisitorId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(Events::Table)
                    .if_not_exists()
                    .col(pk_auto(Events::Id))
                    .col(string(Events::Domain).not_null())
                    .col(string(Events::EventType).not_null())
                    .col(text(Events::PageUrl).not_null())
                    .col(string(Events::PageUrlPath).not_null())
                    .col(text_null(Events::Referrer))
                    .col(string(Events::Source).not_null())
                    .col(string(Events::Browser).not_null())
                    .col(string(Events::OperatingSystem).not_null())
                    .col(string(Events::DeviceType).not_null())
                    .col(string(Events::Country).not_null())
                    .col(string(Events::Region).not_null())
                    .col(string(Events::City).not_null())
                    .col(string_null(Events::UtmSource))
                    .col(string_null(Events::UtmMedium))
                    .col(string_null(Events::UtmCampaign))
                    .col(string_null(Events::UtmContent))
                    .col(string_null(Events::UtmTerm))
                    .col(timestamp_with_time_zone(Events::Timestamp).not_null())
                    .col(string(Events::VisitorId).not_null())
                    .col(json_binary_null(Events::CustomParams))
                    .col(big_integer(Events::VisitId))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_events_visit_id")
                            .from(Events::Table, Events::VisitId)
                            .to(Visits::Table, Visits::Id)
                            .on_delete(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_events_domain_timestamp")
                    .table(Events::Table)
                    .col(Events::Domain)
                    .col(Events::Timestamp)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_events_domain_visit_id")
                    .table(Events::Table)
                    .col(Events::Domain)
                    .col(Events::VisitId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_events_domain_page_path")
                    .table(Events::Table)
                    .col(Events::Domain)
                    .col(Events::PageUrlPath)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Events::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(Visits::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(DailySalts::Table).to_owned())
            .await?;
        Ok(())
    }
}

#[derive(DeriveIden)]
enum DailySalts {
    Table,
    Day,
    Value,
}

#[derive(DeriveIden)]
enum Visits {
    Table,
    Id,
    Domain,
    VisitorId,
    PageUrl,
    PageUrlPath,
    Referrer,
    Source,
    Browser,
    OperatingSystem,
    DeviceType,
    Country,
    Region,
    City,
    UtmSource,
    UtmMedium,
    UtmCampaign,
    UtmContent,
    UtmTerm,
    StartedAt,
    LastActivityAt,
    LastVisitedUrl,
    LastVisitedUrlPath,
}

#[derive(DeriveIden)]
enum Events {
    Table,
    Id,
    Domain,
    EventType,
    PageUrl,
    PageUrlPath,
    Referrer,
    Source,
    Browser,
    OperatingSystem,
    DeviceType,
    Country,
    Region,
    City,
    UtmSource,
    UtmMedium,
    UtmCampaign,
    UtmContent,
    UtmTerm,
    Timestamp,
    VisitorId,
    CustomParams,
    VisitId,
}

fn string<T: IntoIden>(column: T) -> ColumnDef {
    ColumnDef::new(column).string().not_null().to_owned()
}

fn string_null<T: IntoIden>(column: T) -> ColumnDef {
    ColumnDef::new(column).string().null().to_owned()
}

fn text<T: IntoIden>(column: T) -> ColumnDef {
    ColumnDef::new(column).text().not_null().to_owned()
}

fn text_null<T: IntoIden>(column: T) -> ColumnDef {
    ColumnDef::new(column).text().null().to_owned()
}

fn date<T: IntoIden>(column: T) -> ColumnDef {
    ColumnDef::new(column).date().not_null().to_owned()
}

fn pk_auto<T: IntoIden>(column: T) -> ColumnDef {
    ColumnDef::new(column).big_integer().not_null().auto_increment().primary_key().to_owned()
}

fn big_integer<T: IntoIden>(column: T) -> ColumnDef {
    ColumnDef::new(column).big_integer().null().to_owned()
}

fn json_binary_null<T: IntoIden>(column: T) -> ColumnDef {
    ColumnDef::new(column).json_binary().null().to_owned()
}

fn timestamp_with_time_zone<T: IntoIden>(column: T) -> ColumnDef {
    ColumnDef::new(column)
        .timestamp_with_time_zone()
        .not_null()
        .to_owned()
}
