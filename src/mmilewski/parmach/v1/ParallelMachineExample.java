package mmilewski.parmach.v1;

import com.google.common.collect.Iterators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import static java.lang.System.currentTimeMillis;

public class ParallelMachineExample {
    public static void main(String[] args) throws Exception {
        new ParallelMachineExample().run();
    }

    public void run() throws Exception {
        File inputFile = new File("c:/marcin/samplefile.txt");
        int numOfParallelExecutors = 20;

        // configure machine which will execute tasks in parallel.
        ParallelMachine<ItemDescription, FetchedData> pm = new ParallelMachine<>(numOfParallelExecutors);
        pm.setProcessor(item -> FetchedData.fetchFrom(item.stringDescription));
        pm.setCallbacks(
            (item, result) -> {
                println("item was processed and is ready for you to use. item: " + item.stringDescription.substring(0, 10));
            },
            exception -> {
                println("error " + exception);
                return ParallelMachine.ActionOnError.IGNORE;
            }
        );

        // read file sequentially and process it in parallel
        try (BufferedReader fileReader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)))) {
            long startTimeMillis = currentTimeMillis();
            {
                Iterator<String> inputLinesIt = fileReader.lines().iterator();
                Iterator<ItemDescription> itemDescriptionsIt = Iterators.transform(inputLinesIt, ItemDescription::new);
                pm.processIterator(itemDescriptionsIt);
            }
            System.out.println("Processed all in " + (currentTimeMillis() - startTimeMillis) + " ms");
        }
    }

    private static class ItemDescription {
        private String stringDescription;

        public ItemDescription(String stringDescription) {
            this.stringDescription = stringDescription;
        }
    }

    private static class FetchedData {
        static FetchedData fetchFrom(String dataToConstructRequest) {
            // call a service... and return with results
            return new FetchedData();
        }
    }

    private void println(String s) {
        // printing is slow
//        System.out.println(s);
    }
}

