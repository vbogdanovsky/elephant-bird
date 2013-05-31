package com.twitter.elephantbird.pig.piggybank;

import static com.twitter.data.proto.tutorial.AddressBookProtos.Measurements;
import static com.twitter.data.proto.tutorial.AddressBookProtos.PersonParams;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import com.google.protobuf.ByteString;
import com.twitter.elephantbird.pig.util.ProtobufToPig;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.twitter.data.proto.tutorial.AddressBookProtos.AddressBook;
import com.twitter.data.proto.tutorial.AddressBookProtos.Person;
import com.twitter.elephantbird.pig.util.PigUtil;
import com.twitter.elephantbird.pig.util.ProjectedProtobufTupleFactory;
import com.twitter.elephantbird.pig.util.ProtobufTuple;
import com.twitter.elephantbird.util.TypeRef;

public class TestProtoToPig {

  private static TupleFactory tf_ = TupleFactory.getInstance();

  @Test
  public void testProtoToPig() throws IOException {
    AddressBook abProto = Fixtures.buildAddressBookProto();

    Tuple abProtoTuple = tf_.newTuple(new DataByteArray(abProto.toByteArray()));

    ProtobufBytesToTuple abProtoToPig =
        new ProtobufBytesToTuple(AddressBook.class.getCanonicalName());
    Tuple abTuple = abProtoToPig.exec(abProtoTuple);
    assertEquals("{(Elephant Bird,123,elephant@bird.com,{(415-999-9999,HOME),(415-666-6666,MOBILE),(415-333-3333,WORK)}),(Elephant Bird,123,elephant@bird.com,{(415-999-9999,HOME),(415-666-6666,MOBILE),(415-333-3333,WORK)})}",
        abTuple.toDelimitedString(","));
  }

  @Test
  public void testLazyProtoToPig() throws ExecException {
    Person personProto = Fixtures.buildPersonProto();
    Tuple protoTuple = new ProtobufTuple(personProto);
    Tuple normalTuple = Fixtures.buildPersonTuple();
    List<FieldDescriptor> fieldDescs = personProto.getDescriptorForType().getFields();

    TypeRef<Person> typeRef = PigUtil.getProtobufTypeRef(Person.class.getName());
    Tuple projectedTuple =
      new ProjectedProtobufTupleFactory<Person>(typeRef, evenFields(fieldDescs)).newTuple(personProto);

    int idx = 0;
    for (FieldDescriptor fd : fieldDescs) {
      // Skipping data bags. Data bags actually work; it' just our fixture is not good for this,
      // since it tests "default value" functionality by leaving some elements as null, expecting
      // protobuf conversion to fill the nulls in. Which is what happens. But that means converting back
      // gives us non-null fields, which are not equal to the null fields...
      if (normalTuple.get(fd.getIndex()) instanceof DataBag) {
        continue;
      }
      assertEquals(protoTuple.get(fd.getIndex()), normalTuple.get(fd.getIndex()));
      if (idx%2 == 0) {
        assertEquals(projectedTuple.get(fd.getIndex()/2), normalTuple.get(fd.getIndex()));
      }
      idx++;
    }
  }

  private static RequiredFieldList evenFields(List<FieldDescriptor> protoFields) {
    RequiredFieldList reqList = new RequiredFieldList();

    int i = 0;
    for(FieldDescriptor fd : protoFields) {
      if (i%2 == 0) {
        RequiredField field = new RequiredField();
        field.setAlias(fd.getName());
        field.setIndex(i);
        // field.setType() type is not used
        reqList.add(field);
      }
      i++;
    }
    return reqList;
  }

    @Test
    public void testProtoToPigWithUndefinedFields() throws Exception {
        Measurements.Builder measurementsBuilder = Measurements.newBuilder();
        Measurements measurements = measurementsBuilder.build();

        Tuple tuple = new ProtobufToPig().toTuple(measurements);
        assertEquals("(,,,,{},)", tuple.toString());

        tuple = new ProtobufTuple(measurements);
        assertEquals("(,,,,{},)", tuple.toString());

        PersonParams.Builder personParamsBuilder = PersonParams.newBuilder();
        personParamsBuilder.setMeasure(measurementsBuilder);
        PersonParams personParams = personParamsBuilder.build();

        tuple = new ProtobufToPig().toTuple(personParams);
        assertEquals("((,,,,{},),{},,20)", tuple.toString());

        tuple = new ProtobufTuple(personParams);
        assertEquals("((,,,,{},),{},,20)", tuple.toString());

        measurementsBuilder.setHeight(14).addCoin(5).addCoin(15)
                           .setSign(ByteString.copyFromUtf8("asd"));
        measurements = measurementsBuilder.build();
        tuple = new ProtobufToPig().toTuple(measurements);
        assertEquals("(,14,,,{(5),(15)},asd)", tuple.toString());

        tuple = new ProtobufTuple(measurements);
        assertEquals("(,14,,,{(5),(15)},asd)", tuple.toString());

        personParamsBuilder.setAdult(true).setAge(50)
                           .addPhoneType(Person.PhoneType.MOBILE)
                           .addPhoneType(Person.PhoneType.HOME);
        personParamsBuilder.setMeasure(measurementsBuilder);
        personParams = personParamsBuilder.build();

        tuple = new ProtobufToPig().toTuple(personParams);
        assertEquals("((,14,,,{(5),(15)},asd),{(MOBILE),(HOME)},1,50)", tuple.toString());

        tuple = new ProtobufTuple(personParams);
        assertEquals("((,14,,,{(5),(15)},asd),{(MOBILE),(HOME)},1,50)", tuple.toString());
    }
}
