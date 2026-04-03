use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "visits")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub domain: String,
    pub visitor_id: String,
    pub page_url: String,
    pub page_url_path: String,
    pub referrer: Option<String>,
    pub source: String,
    pub browser: String,
    pub operating_system: String,
    pub device_type: String,
    pub country: String,
    pub region: String,
    pub city: String,
    pub utm_source: Option<String>,
    pub utm_medium: Option<String>,
    pub utm_campaign: Option<String>,
    pub utm_content: Option<String>,
    pub utm_term: Option<String>,
    pub started_at: DateTime<Utc>,
    pub last_activity_at: DateTime<Utc>,
    pub last_visited_url: Option<String>,
    pub last_visited_url_path: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::events::Entity")]
    Events,
}

impl Related<super::events::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Events.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
