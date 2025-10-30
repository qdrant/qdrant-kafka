package io.qdrant.kafka;

import io.qdrant.client.grpc.JsonWithInt.ListValue;
import io.qdrant.client.grpc.JsonWithInt.Struct;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.NamedVectors;
import io.qdrant.client.grpc.Points.SparseIndices;
import io.qdrant.client.grpc.Points.Vector;
import io.qdrant.client.grpc.Points.Vectors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.DataException;

/* Helper to convert JSON vector representations into io.qdrant.client.grpc.Points.Vectors. */

// Example JSON inputs:
// {
//   "vector": [
//     0.041732933,
//     0.013779674,
//     -0.027564144
//   ]
// }

// {
//   "vector": {
//     "some-name": [
//       0.041732933,
//       0.013779674,
//       -0.027564144
//     ]
//   }
// }

// {
//   "vector": {
//     "some-name": {
//       "indices": [
//         32,
//         532,
//         5133
//       ],
//       "values": [
//         0.041732933,
//         0.013779674,
//         -0.027564144
//       ]
//     }
//   }
// }

// {
//   "vector": {
//     "some-name": [
//       [0.041732933, 0.013779674, -0.027564144],
//       [0.051345434, 0.013743223, -0.027576543],
//       [0.041732933, 0.013779674, -0.027564144]
//     ]
//   }
// }

class VectorsFactory {

  public static Vectors vectors(Value vectorValue) throws DataException {
    Vectors.Builder vectorsBuilder = Vectors.newBuilder();

    // Primitive dense vectors check
    if (vectorValue.hasListValue()) {
      vectorsBuilder.setVector(parseDenseVector(vectorValue.getListValue()));

      // NamedVectors check
    } else if (vectorValue.hasStructValue()) {
      vectorsBuilder.setVectors(parseNamedVectors(vectorValue.getStructValue()));
    } else {
      throw new DataException("Invalid vector format");
    }

    return vectorsBuilder.build();
  }

  private static NamedVectors parseNamedVectors(Struct struct) throws DataException {
    NamedVectors.Builder namedVectorsBuilder = NamedVectors.newBuilder();
    for (Map.Entry<String, Value> entry : struct.getFieldsMap().entrySet()) {
      String key = entry.getKey();
      Value value = entry.getValue();
      if (value.hasListValue()) {
        namedVectorsBuilder.putVectors(key, parseDenseVector(value.getListValue()));
      } else if (value.hasStructValue()) {
        namedVectorsBuilder.putVectors(key, parseSparseVector(value.getStructValue()));
      } else {
        throw new DataException("Named vector values must be either dense or sparse vectors");
      }
    }
    return namedVectorsBuilder.build();
  }

  private static Vector parseDenseVector(ListValue listValue) throws DataException {
    if (listValue.getValuesCount() == 0) {
      throw new DataException("Dense vector cannot be empty");
    }

    Vector.Builder vectorBuilder = Vector.newBuilder();
    for (Value value : listValue.getValuesList()) {
      if (value.hasListValue()) {
        return parseMultiDenseVector(listValue);
      }

      if (value.hasDoubleValue()) {
        vectorBuilder.addData((float) value.getDoubleValue());
      } else if (value.hasIntegerValue()) {
        vectorBuilder.addData((float) value.getIntegerValue());
      } else {
        throw new DataException("Dense vector data must be a list of numbers");
      }
    }
    return vectorBuilder.build();
  }

  private static Vector parseMultiDenseVector(ListValue listValue) throws DataException {
    if (listValue.getValuesCount() == 0) {
      throw new DataException("Multi vector cannot be empty");
    }

    Vector.Builder vectorBuilder = Vector.newBuilder();
    int numRows = listValue.getValuesCount();
    int expectedDimension = -1;

    for (Value row : listValue.getValuesList()) {
      if (!row.hasListValue()) {
        throw new DataException("Multi vector data must be a list of lists of floats");
      }

      int rowDimension = row.getListValue().getValuesCount();
      if (rowDimension == 0) {
        throw new DataException("Multi vector rows cannot be empty");
      }

      if (expectedDimension == -1) {
        expectedDimension = rowDimension;
      } else if (rowDimension != expectedDimension) {
        throw new DataException(
            String.format(
                "Multi vector rows must have consistent dimensions (expected: %d, found: %d)",
                expectedDimension, rowDimension));
      }

      for (Value value : row.getListValue().getValuesList()) {
        if (value.hasDoubleValue()) {
          vectorBuilder.addData((float) value.getDoubleValue());
        } else if (value.hasIntegerValue()) {
          vectorBuilder.addData((float) value.getIntegerValue());
        } else {
          throw new DataException("Multi vector data must be a list of lists of numbers");
        }
      }
    }
    vectorBuilder.setVectorsCount(numRows);
    return vectorBuilder.build();
  }

  private static Vector parseSparseVector(Struct struct) throws DataException {
    Map<String, Value> fields = struct.getFieldsMap();

    if (!fields.containsKey("indices") || !fields.containsKey("values")) {
      throw new DataException("Sparse vector must contain 'indices' and 'values' fields");
    }

    Value valuesField = fields.get("values");
    Value indicesField = fields.get("indices");

    if (!valuesField.hasListValue()) {
      throw new DataException("Sparse vector 'values' field must be a list");
    }

    if (!indicesField.hasListValue()) {
      throw new DataException("Sparse vector 'indices' field must be a list");
    }

    Vector.Builder vectorBuilder = Vector.newBuilder();
    SparseIndices.Builder sparseIndicesBuilder = SparseIndices.newBuilder();

    ListValue valuesValue = valuesField.getListValue();
    vectorBuilder = parseDenseVector(valuesValue).toBuilder();

    List<Integer> indicesList = new ArrayList<>();
    ListValue indicesValue = indicesField.getListValue();
    for (Value value : indicesValue.getValuesList()) {
      if (!value.hasIntegerValue()) {
        throw new DataException("Indices must be a list of integers");
      }
      indicesList.add((int) value.getIntegerValue());
    }

    if (indicesList.size() != valuesValue.getValuesCount()) {
      throw new DataException(
          String.format(
              "Sparse vector 'indices' and 'values' must have the same length (indices: %d, values:"
                  + " %d)",
              indicesList.size(), valuesValue.getValuesCount()));
    }

    sparseIndicesBuilder.addAllData(indicesList);
    vectorBuilder.setIndices(sparseIndicesBuilder.build());

    return vectorBuilder.build();
  }
}
