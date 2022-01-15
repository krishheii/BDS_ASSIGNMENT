const yargs = require("yargs");
const fs = require("fs");
const mongoose = require("mongoose");

main().catch((err) => console.log(err));

async function main() {
  await mongoose.connect("mongodb://localhost:27017/test");
}

const restaurantSchema = new mongoose.Schema({
  zipCode: String,
  name: String,
  rating: Number,
  type_of_food: String,
  address: String,
});

const Restaurant = mongoose.model("Restaurant", restaurantSchema);

const loaderFunction = async () => {
  fs.readFile("./res.json", "utf8", (err, jsonString) => {
    if (err) {
      console.log("Error reading file from disk:", err);
      return;
    }
    try {
      const datas = JSON.parse(jsonString);
      let count = 0;
      datas.map(async (data) => {
        const saveData = {
          zipCode: data.postcode + data.outcode,
          name: data.name,
          _id: data._id.$oid,
          rating: data.rating,
          type_of_food: data.type_of_food,
          address: data.address,
        };
        const restaurantData = new Restaurant(saveData);
        await restaurantData.save();
        console.log("Inserted one query: ", count++);
        if (datas.length - 1 === count) {
          await fetcher();
        }
      });
    } catch (err) {
      console.log("Error parsing JSON string:", err);
    }
  });
};

const fetcher = async () => {
  const firstCriteria = { zipCode: "0QRCH4", type_of_food: "Curry" };
  const firstTestCasse = await Restaurant.find(firstCriteria)
    .sort({ rating: -1 })
    .limit(5);
  console.log("Data Loader Completed\n\n");
  console.log(
    "-------------------------------------Test Case 1---------------------------------"
  );
  console.log(firstTestCasse);
  const secondCriteria = { address: /30/, qty: { $gt: 4 } };
  const secondTestCriteria = await Restaurant.find(secondCriteria).sort({
    rating: -1,
  });
  console.log(
    "-------------------------------------Test Case 2---------------------------------"
  );
  console.log(secondTestCriteria);
  const thirdCriteria = [
    { $match: { type_of_food: "Thai" } },
    { $group: { _id: "$zipCode", count: { $sum: 1 } } },
  ];
  const thirdTestCriteria = await Restaurant.aggregate(thirdCriteria).sort({
    rating: -1,
  });
  console.log(
    "-------------------------------------Test Case 3---------------------------------"
  );
  console.log(thirdTestCriteria);
  const fourthCriteria = [
    {
      $group: {
        _id: "$type_of_food",
        avgRating: { $avg: "$rating" },
      },
    },
  ];

  const fourthTestCriteria = await Restaurant.aggregate(fourthCriteria).sort({
    rating: -1,
  });
  console.log(
    "-------------------------------------Test Case 4---------------------------------"
  );
  console.log(fourthTestCriteria);
};

// Create add command
yargs.command({
  command: "loader",
  describe: "Database Loader",
  builder: {
    file: {
      describe: "Loader",
      demandOption: true,
      type: "string",
    },
  },

  handler(argv) {
    if (argv.file === "json") {
      loaderFunction();
    }
  },
});

yargs.parse(); // To set above changes
