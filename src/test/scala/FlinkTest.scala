import com.poly.flink.utils.Schemas
import org.apache.orc._
import org.junit.Assert.assertTrue
import org.junit.{Before, Test}


// scalatest with JUnit 4
//@RunWith(classOf[JUnitRunner])
class FlinkTest {

  /* underscore Default initializer */
  var schema: TypeDescription = _

  @Before
  def fixtureArray() {
    val schemaString: String = "struct<name:string,level:string,city:string,county:string,state:string,country:string,population:string,latitude:double,longitude:double,aggregate:string,timezone:string,cases:string,us_confirmed_county:string,deaths:string,us_deaths_county:string,recovered:string,us_recovered_county:string,active:string,us_active_county:string,tested:string,hospitalized:string,discharged:string,last_updated:date,icu:string>"
    schema = TypeDescription.fromString(schemaString)

  }

  @Test
  def test_LastUpdateDate_Date_Type(): Unit = {
    val validate: Schemas = new Schemas()
    assertTrue("%s%s".format("Success, type is:",
      schema.getChildren.get(22).toString),
      schema.getChildren.get(22).equals(validate.getCovidSchema().getSchema.getChildren.get(22))
    )
    println("%s%s%s%s".format("Success, type is: ", schema.getChildren.get(22).toString, " for -> ", schema.getFieldNames.get(22)))
  }

  @Test
  def test_DoubleFields_Type(): Unit = {
    val validate: Schemas = new Schemas()

    assertTrue("%s%s".format("type is:",
      schema.getChildren.get(8).toString),
      schema.getChildren.get(8).equals(validate.getCovidSchema().getSchema.getChildren.get(8))
    )
    println("%s%s%s%s".format("type is: ", schema.getChildren.get(8).toString, " for -> ", schema.getFieldNames.get(8)))

    assertTrue("%s%s".format("type is:",
      schema.getChildren.get(7).toString),
      schema.getChildren.get(7).equals(validate.getCovidSchema().getSchema.getChildren.get(7))
    )

    println("%s%s%s%s".format("type is: ", schema.getChildren.get(7).toString, " for -> ", schema.getFieldNames.get(7)))
  }


}
