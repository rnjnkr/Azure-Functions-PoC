package com.poc.azure.blobtrigger;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

import java.util.stream.Stream;

public class Function {
    @FunctionName("BlobTrigger-Java")
    public void run (@BlobTrigger(name = "myBlobTrigger", path = "sample-blob-triggers/{name}"
            , connection = "AzureWebJobsStorage")  byte[] content,
                     @BindingName("name") String fileName, final ExecutionContext context) {
        context.getLogger().info("Function invoked for new blob. Name of file: " + fileName + "\n"
                + "Size of file: " + content.length);
    }

}
