use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "events")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub domain: String,
    pub event_type: String,
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
    pub timestamp: DateTime<Utc>,
    pub visitor_id: String,
    pub custom_params: Option<Json>,
    pub visit_id: Option<i64>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::visits::Entity",
        from = "Column::VisitId",
        to = "super::visits::Column::Id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Visit,
}

impl Related<super::visits::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Visit.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
