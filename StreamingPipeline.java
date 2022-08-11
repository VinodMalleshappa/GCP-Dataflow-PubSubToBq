package org.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * Hello world!
 *
 */
public class StreamingPipeline
{
    public static void main( String[] args )
    {
        //Begin with the Pipeline Options
        DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        dataflowPipelineOptions.setJobName("StreamingIngestionPubSubToBq"); //Set the JobName
        dataflowPipelineOptions.setProject("nttdata-c4e-bde");  //Adding the project details
        dataflowPipelineOptions.setRegion("europe-west4");      //Region is added
        dataflowPipelineOptions.setGcpTempLocation("gs://c4e-uc1-dataflow-temp-2/temp"); //added the Storage temp table
        dataflowPipelineOptions.setRunner(DataflowRunner.class); //Dataflow runner

        //Creating Pipeline from Options
        Pipeline pipeline = Pipeline.create(dataflowPipelineOptions);
        //Publishing message from pubsub
        PCollection<String> pubsubmessage = pipeline.apply(PubsubIO.readStrings().fromTopic("projects/nttdata-c4e-bde/topics/uc1-input-topic-2"));

        //Reading the message from PubSub
        PCollection<TableRow> bqrow=pubsubmessage.apply(ParDo.of(new ConvertorStringBq()));

        //Write Bqrow into Bigquery
        bqrow.apply(BigQueryIO.writeTableRows().to("nttdata-c4e-bde:uc1_2.account")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER) //writing into existing table
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));  //Append into the table

        pipeline.run(); //Running the pipeline is last step
    }
    public static class ConvertorStringBq extends org.apache.beam.sdk.transforms.DoFn<String, TableRow> {
        @ProcessElement
        public void Processing(ProcessContext processContext){
            TableRow tableRow =new TableRow().set("id",processContext.element()) //.toString()
                    .set("name",processContext.element().toString())
                    .set("surname",processContext.element().toString());
            processContext.output(tableRow);
        }
    }
}
