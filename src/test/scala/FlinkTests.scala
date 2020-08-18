import com.poly.flink.utils.Schemas
import org.apache.orc._
import org.junit.Assert.assertTrue
import org.junit.{Before, Test}

// scalatest with JUnit 4
class FlinkTests {

  /* underscore Default initializer */
  var schema: TypeDescription = _

  @Before
  def fixtureArray() {
    val schemaString: String = "struct<name:string,level:string,city:string,county:string,state:string,country:string,population:string,latitude:double,longitude:double,aggregate:string,timezone:string,cases:string,us_confirmed_county:string,deaths:string,us_deaths_county:string,recovered:string,us_recovered_county:string,active:string,us_active_county:string,tested:string,hospitalized:string,discharged:string,last_updated:date,icu:string>"
    schema = TypeDescription.fromString(schemaString)

  }

  @Test
  def test_LastUpdateDate_Date_Type() {
    val validate: Schemas = new Schemas()
    assertTrue(schema.getChildren.get(22).equals(validate.getCovidSchema().getSchema.getChildren.get(22)))
  }


}
