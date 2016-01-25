package software.uncharted.graphing.tiling



import java.lang.{Double => JavaDouble}
import java.io.{OutputStream, InputStream}
import java.util.{List => JavaList}

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

import com.oculusinfo.binning.impl.DenseTileData
import com.oculusinfo.binning.util.TypeDescriptor
import com.oculusinfo.binning.{TileData, TileIndex}
import com.oculusinfo.binning.io.serialization.TileSerializer
import com.oculusinfo.factory.properties.StringProperty
import com.oculusinfo.factory.ConfigurableFactory
import com.oculusinfo.tilegen.datasets._
import com.oculusinfo.tilegen.tiling.analytics.{NumericStatsBinningAnalytic, NumericMeanBinningAnalytic, NumericBinningAnalyticFactory, BinningAnalytic}
import com.oculusinfo.tilegen.util.{TypeConversion, ExtendedNumeric}



object HeatmapSerializer {
  private val BytesPerDouble = 8
}
class HeatmapSerializer extends TileSerializer[JavaDouble] {
  import HeatmapSerializer._

  override def getBinTypeDescription: TypeDescriptor = new TypeDescriptor(classOf[JavaDouble])

  override def serialize(tile: TileData[JavaDouble], output: OutputStream): Unit = {
    val defn = tile.getDefinition
    val result = new Array[Byte](defn.getXBins * defn.getYBins * BytesPerDouble)
    var resultIndex = 0

    for (y <- 0 until defn.getYBins; x <- 0 until defn.getXBins) {
      val data = JavaDouble.doubleToLongBits(tile.getBin(x, y).doubleValue())
      for (i <- 0 to 7) {
        result(resultIndex) = ((data >> (i * 8)) & 0xff).asInstanceOf[Byte]
        resultIndex += 1
      }
    }

    output.write(result)
  }

  override def deserialize(index: TileIndex, rawData: InputStream): TileData[JavaDouble] = {
    val buffer = new Array[Byte](index.getXBins * index.getYBins * BytesPerDouble)
    rawData.read(buffer)

    val result = new DenseTileData[JavaDouble](index)

    var bufferIndex = 0
    for (y <- 0 until index.getYBins; x <- 0 until index.getXBins) {
      var value: Long = 0
      for (i <- 0 to 7) {
        value = value + buffer(bufferIndex) << (i * 8)
        bufferIndex += 1
      }
      result.setBin(x, y, JavaDouble.longBitsToDouble(value))
    }

    result
  }
}


class HeatmapCountValueExtractor extends CountValueExtractor[Double, JavaDouble](new HeatmapSerializer)
class HeatmapFieldValueExtractor (field: String, binningAnalytic: BinningAnalytic[Double, JavaDouble]) extends FieldValueExtractor(field, binningAnalytic, new HeatmapSerializer)

object HeatmapCountValueExtractorFactory {
  private[tiling] val NAME="newcount"

  def register =
    ValueExtractorFactory.addSubFactoryProvider(
      FactoryKey(NAME, classOf[HeatmapCountValueExtractorFactory]),
      ValueExtractorFactory.subFactoryProvider { (parent, path) =>
        new HeatmapCountValueExtractorFactory(parent, path)
      }
    )
}

class HeatmapCountValueExtractorFactory (parent: ConfigurableFactory[_], path: JavaList[String])
extends ValueExtractorFactory(HeatmapCountValueExtractorFactory.NAME, parent, path) {
  override protected def typedCreate[T, JT] (tag: ClassTag[T],
                                             numeric: ExtendedNumeric[T],
                                             conversion: TypeConversion[T, JT]): ValueExtractor[_, _] = {
    new HeatmapCountValueExtractor
  }
}

object HeatmapFieldValueExtractorFactory {
  private[tiling] val NAME = "newfield"
  private[tiling] val FIELD_PROPERTY =
    new StringProperty("field", "The field used by this value extractor", "INVALID FIELD")

  def register =
    ValueExtractorFactory.addSubFactoryProvider(
      FactoryKey(NAME, classOf[HeatmapFieldValueExtractorFactory]),
      ValueExtractorFactory.subFactoryProvider { (parent, path) =>
        new HeatmapFieldValueExtractorFactory(parent, path)
      }
    )
}
class HeatmapFieldValueExtractorFactory (parent: ConfigurableFactory[_], path: JavaList[String])
extends ValueExtractorFactory(HeatmapFieldValueExtractorFactory.NAME, parent, path) {
  import FieldValueExtractorFactory._

  addProperty(HeatmapFieldValueExtractorFactory.FIELD_PROPERTY)
  addChildFactory(new NumericBinningAnalyticFactory(this, List[String]().asJava))

  override protected def typedCreate[T, JT] (tag: ClassTag[T],
                                             numeric: ExtendedNumeric[T],
                                             conversion: TypeConversion[T, JT]): ValueExtractor[_, _] = {
    val field = getPropertyValue(HeatmapFieldValueExtractorFactory.FIELD_PROPERTY)
    // Guaranteed the same numeric type because the numeric type of the analytic will be determined by the exact
    // same property by which our numeric type is determined.
    val analytic = produce(classOf[BinningAnalytic[T, JT]])

    if (analytic.isInstanceOf[NumericMeanBinningAnalytic[_]]) {
      val serializer = new HeatmapSerializer
      new MeanValueExtractor[T](field, analytic.asInstanceOf[NumericMeanBinningAnalytic[T]], serializer)(tag, numeric)
    } else if (analytic.isInstanceOf[NumericStatsBinningAnalytic[_]]) {
      throw new IllegalArgumentException("Stats analytic not implemented at the moment")
    } else {
      val serializer = (new HeatmapSerializer).asInstanceOf[TileSerializer[JT]]
      new FieldValueExtractor[T, JT](field, analytic, serializer)(tag, numeric, conversion)
    }
  }
}