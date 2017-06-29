using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.Linq;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace json_transform_for_couchdb
{
    /// <summary>
    /// This program simulates the transformation of the data from one of the system caches (in this example the ID card db), and 
    /// how that gets transformed into the JSON format required to store it in the metaverse (couchDB instance)
    /// 
    /// It takes a json file as an input with a list of properties, the name of the property being the key and the value is the value. Property names must be unique. Nested items
    /// are allowed (nested item must be valid JObject)
    /// 
    /// It uses a map file, each destination db is represented as a node on the root "map" object (the key of which should be the Db name)
    /// on the nodes, the keys are the destination field name, value is the source field name
    /// 
    /// It outputs a JSON object which could then be fed into CouchDB by another process (each node of the output JSON would go into a DB specified in the key name)
    /// 
    /// </summary>
    public class jsontransform
    {
        public static void Main()
        {            
            //Prepare the JSON from the cache db (for testing only it's coming straight from ID card db)
            JObject inputJSON = PrepareInputJSON();

            //Load the map file (in reality this would be loading an external JSON file)
            JObject mapJSON = LoadMapFile();
            
            //Perform the transform to the format specified in the map file
            var output = TransformJSON(inputJSON, mapJSON);
            //var output2 = TransformJSONAlt(inputJSON, mapJSON);

            //Output results to console
            Console.WriteLine("Excution completed, resulting JSON object ready for input into CouchDB:");
            Console.WriteLine(output.ToString());
            Console.ReadLine();
        }

        private static JObject TransformJSON(JObject inputJSON, JObject mapJSON)
        {
            JObject result = new JObject();
            
            //iterate through the items in the map file
            foreach (var dbCategory in mapJSON)
            {
                var category = dbCategory.Key;
                
                JObject dbItem = new JObject();                
                
                JObject subitems = JObject.Parse(dbCategory.Value.ToString());

                foreach (var subitem in subitems)
                {
                    string propertyName = subitem.Value.Value<string>();
                    string destinationPropertyName = subitem.Key;
                    dynamic matchingValue = null;

                    //check for nested object
                    JToken matchingElementFromInput = inputJSON[propertyName];
                    int numChildren = matchingElementFromInput.Children().Count();

                    if (numChildren > 0)
                    {
                        try
                        {
                            JObject jo = JObject.Parse(inputJSON[propertyName].ToString());
                            matchingValue = jo;
                        }
                        catch
                        {
                            Console.WriteLine($"Error parsing JSON for property {propertyName}!");
                        }
                    }
                    else
                    {
                        //find matching value in input JSON, if any
                        matchingValue = inputJSON[propertyName].Value<string>() ?? string.Empty;
                    }
                                        
                    //create the item
                    dbItem.Add(destinationPropertyName, matchingValue);
                }

                //add the collection of properties for the current database as a node to the output
                result.Add(new JProperty(category, dbItem));
            }

            return result;
        }       

        //returns a JSON map file, this would probably be loaded from external .json file in reality
        //Each destination db is represented as a node on the root "map" object (the key of which should be the Db name)
        //On the nodes, the keys are the destination field name, value is the source field name
        private static JObject LoadMapFile()
        {
            JObject map = new JObject();
                        
            JObject person = new JObject();
            person.Add(new JProperty("firstname", "firstname"));
            person.Add(new JProperty("lastname", "lastname"));
            person.Add(new JProperty("email", "email"));
            person.Add(new JProperty("userID", "userID"));
            person.Add(new JProperty("usercategory", "usercategory"));
            person.Add(new JProperty("photolastupdated", "lastupdated"));

            //this is for testing nested objects, enable the relevant part of PrepareInputJSON() too if using
            person.Add(new JProperty("sgs", "sgs"));

            map.Add(new JProperty("person", person));

            JObject card = new JObject();
            card.Add(new JProperty("mifare", "currentMifareID"));
            card.Add(new JProperty("idnumber", "userID"));
            card.Add(new JProperty("lastupdated", "cardlastupdated"));

            map.Add( new JProperty("card",card));

            return map;
        }

        //This prepares the input json from the 3 ID card db tables. In reality this would be prepared by an interface program
        //sitting between the cache and the transform process
        private static JObject PrepareInputJSON()
        {
            IDCardCard card;
            IDCardPhoto photo;
            IDCardUser user;

            //retrieve the records for supplied user
            using (IDCardsDataContext dc = new IDCardsDataContext())
            {
                card = dc.Cards.Where(a => a.UserID.Equals("5061011")).Select(a => new IDCardCard(a.MifareID, a.UserID, a.LastUpdated)).FirstOrDefault();
                user = dc.Users.Where(a => a.IDNumber.Equals("5061011")).Select(a => new IDCardUser(a.IDNumber, a.OtherAccountID, a.FirstName, a.LastName, a.DisplayName, a.Department, a.Email, a.UserCategory, a.Tag, a.CurrentMifareID, a.Flag, a.Deleted, a.CardLastPrinted, a.LastUpdated)).FirstOrDefault();
                photo = dc.Photos.Where(a => a.ID.Equals("5061011")).Select(a => new IDCardPhoto(a.ID, a.Length, a.Picture, a.Type, a.UserID, a.ApprovedState, a.LastUpdated, a.PhotoID)).FirstOrDefault();
            }

            

            //convert to json objects
            JObject json = new JObject();
            json.Add(new JProperty("mifareID", card.MifareID));
            json.Add(new JProperty("cardlastupdated", card.LastUpdated));
            json.Add(new JProperty("lastupdated", photo.LastUpdated));
            json.Add(new JProperty("cardLastPrinted", user.CardLastPrinted));
            json.Add(new JProperty("currentMifareID", user.CurrentMifareID));
            json.Add(new JProperty("deleted", user.Deleted));
            json.Add(new JProperty("department", user.Department));
            json.Add(new JProperty("displayname", user.DisplayName));
            json.Add(new JProperty("email", user.Email));
            json.Add(new JProperty("firstname", user.FirstName));
            json.Add(new JProperty("lastname", user.LastName));
            json.Add(new JProperty("otheraccountid", user.OtherAccountID));
            json.Add(new JProperty("tag", user.Tag));
            json.Add(new JProperty("usercategory", user.UserCategory));
            json.Add(new JProperty("userID", user.IDNumber));            
            json.Add(new JProperty("mifare", card.MifareID));
            json.Add(new JProperty("userid", card.UserID));

            //code for testing nested objects
            JObject sgs = new JObject();
            sgs.Add("sg1", "sg1val");
            sgs.Add("sg2", "sg2val");
            sgs.Add("sg3", "sg3val");
            json.Add(new JProperty("sgs", sgs));

            return json;
        }
    }

    //represents one record from the IDCard.Users table
    public class IDCardUser
    {
        public IDCardUser(string iDNumber, string otherAccountID, string firstName, string lastName, string displayName, string department, string email, string userCategory, int? tag, string currentMifareID, int? flag, bool deleted, DateTime? cardLastPrinted, DateTime? lastUpdated)
        {
            this.IDNumber = iDNumber;
            this.OtherAccountID = otherAccountID;
            this.FirstName = firstName;
            this.LastName = lastName;
            this.DisplayName = displayName;
            this.Department = department;
            this.Email = email;
            this.UserCategory = userCategory;
            this.Tag = tag;
            this.CurrentMifareID = currentMifareID;
            this.Flag = flag;
            this.Deleted = deleted;
            this.CardLastPrinted = cardLastPrinted;
            this.LastUpdated = lastUpdated;
        }

        public string IDNumber { get; set; }
        public string OtherAccountID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string DisplayName { get; set; }
        public string Department { get; set; }
        public string Email { get; set; }
        public string UserCategory { get; set; }
        public int? Tag { get; set; }
        public string CurrentMifareID { get; set; }
        public int? Flag { get; set; }
        public bool Deleted { get; set; }
        public DateTime? CardLastPrinted { get; set; }
        public DateTime? LastUpdated { get; set; }


    }

    //represents one record from the IDCard.Cards table
    public class IDCardCard
    {
        public IDCardCard(string mifareID, string userID, DateTime lastUpdated)
        {
            MifareID = mifareID;
            UserID = userID;
            LastUpdated = lastUpdated;
        }

        public string MifareID { get; set; }
        public string UserID { get; set; }
        public DateTime LastUpdated { get; set; }
    }

    //represents one record from the IDCard.Photos table
    public class IDCardPhoto
    {
        public int ID { get; set; }
        public int Length { get; set; }
        public Binary Picture { get; set; }
        public int Type { get; set; }
        public string UserID { get; set; }
        public string ApprovedState { get; set; }
        public DateTime LastUpdated { get; set; }
        public int PhotoID { get; set; }

        public IDCardPhoto() { }

        public IDCardPhoto(int id, int length, Binary picture, int type, string userid, string approvedstate, DateTime lastupdated, int photoid)
        {
            this.ID = id;
            this.Length = length;
            this.Picture = picture;
            this.Type = type;
            this.UserID = userid;
            this.ApprovedState = approvedstate;
            this.LastUpdated = lastupdated;
            this.PhotoID = photoid;
        }
    }
}
