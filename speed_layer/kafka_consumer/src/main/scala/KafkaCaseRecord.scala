import scala.reflect.runtime.universe._


case class KafkaCaseRecord(
                              case_id: String,
                              charge_count: String,
                              age_at_incident: String,
                              bond_amount_current: String,
                              judge: String,
                              unit: String,
                              gender: String,
                              race: String,
                              court_name: String,
                              offense_category: String,
                              disposition_charged_offense_title: String)