package q3df.mil;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


/**
 * MilMainClass.
 *
 * @author Mikita Stryha <m.stryha@sam-solutions.com>
 */
public class MilMainClass
{
	private static final String COLLECTION_NAME = "movies";
	private static final String SERVER_URL = "http://localhost:8983/solr/";

	public static void main(String[] args) throws IOException, SolrServerException
	{
		// 1000
		//		List<Product> products = generateProducts();

		// 28795
		//		List<Movies> movies = Movies.getMovies();

		// 11269
		//		List<ProductsV1> productsV1 = ProductsV1.getProductsV1();

		//633719 - 100%
		// -Xmx6006m -Xms512m if we want to add 100% of document at once
		List<MovieV1> moviesV1 = MovieV1.getMoviesV1(100);

		addDocs(moviesV1);

		//		for (SolrDocument next : simpleSolrQuery(solr, "*:*", 10)) {
		//			prettyPrint(next);
		//		}
	}

	static void addDocs(Collection<?> data) throws IOException, SolrServerException
	{
		final SolrServer solr = new HttpSolrServer(SERVER_URL + COLLECTION_NAME);

		int i = 0;
		if (data.size() > 10000)
		{
			List storage = new ArrayList<>(10000);
			for (Object doc : data)
			{
				storage.add(doc);
				if (i++ == 10000)
				{
					solr.addBeans(storage);
					storage = new ArrayList(10000);
				}
			}
			solr.addBeans(storage);
		}

		solr.commit();
	}

	static SolrDocumentList simpleSolrQuery(SolrServer solr,
			String query, int rows) throws SolrServerException
	{
		SolrQuery solrQuery = new SolrQuery(query);
		solrQuery.setRows(rows);
		QueryResponse resp = solr.query(solrQuery);
		return resp.getResults();
	}

	static void prettyPrint(SolrDocument doc)
	{
		List<String> sortedFieldNames =
				new ArrayList<String>(doc.getFieldNames());
		Collections.sort(sortedFieldNames);
		System.out.println();
		for (String field : sortedFieldNames)
		{
			System.out.println(String.format("\t%s: %s", field, doc.getFieldValue(field)));
		}
		System.out.println();
	}




	private static final AtomicLong productCounter = new AtomicLong(0L);
	private static final AtomicLong moviesCounter = new AtomicLong(0L);
	private static final AtomicLong productV1Counter = new AtomicLong(0L);

	private static List<Product> generateProducts()
	{
		final Random random = new Random(47);
		final List<Product> products = new ArrayList<Product>(1000);
		final List<String> descriptions = new ArrayList<String>(500);
		final Labels[] labels = Labels.values();

		BufferedReader br = null;
		BufferedReader brDescriptions = null;

		try
		{
			String descriptionLine;
			brDescriptions = new BufferedReader(new InputStreamReader(MilMainClass.class.getResourceAsStream("/description.csv")));
			while ((descriptionLine = brDescriptions.readLine()) != null)
			{
				descriptions.add(descriptionLine);
			}

			String line;
			br = new BufferedReader(new InputStreamReader(MilMainClass.class.getResourceAsStream("/MOCK_DATA.csv")));
			while ((line = br.readLine()) != null)
			{
				line = line.replaceAll("\"", "");
				String[] elements = line.split(",");

				Product product = new Product();
				product.setId(productCounter.incrementAndGet());
				product.setName(elements[3]);
				product.setPrice(Double.valueOf(elements[2]));
				product.setWeight(Double.valueOf(elements[1]));
				product.setCity(elements[0]);
				product.setDescription(descriptions.get(random.nextInt(descriptions.size())));

				// labels
				int r = random.nextInt(labels.length);
				final Set<Labels> productLabels = new HashSet<Labels>(r);
				if (Math.random() > 0.5)
				{
					productLabels.addAll(Arrays.asList(labels).subList(0, r));
				}
				else
				{
					productLabels.addAll(Arrays.asList(labels).subList(labels.length - r, labels.length));
				}

				product.setLabels(new ArrayList<Labels>(productLabels));

				products.add(product);
			}
		}
		catch (IOException exception)
		{
			exception.printStackTrace();
		}
		finally
		{
			if (br != null)
			{
				try
				{
					br.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
			if (brDescriptions != null)
			{
				try
				{
					brDescriptions.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}

		}

		return products;
	}

	private static class Product
	{
		@Field
		private Long id;
		@Field
		private String name;
		@Field
		private Double price;
		@Field
		private Double weight;
		@Field
		private String city;
		@Field
		private String description;
		@Field
		private List<Labels> labels;

		public Long getId()
		{
			return id;
		}

		public void setId(Long id)
		{
			this.id = id;
		}

		public String getName()
		{
			return name;
		}

		public void setName(String name)
		{
			this.name = name;
		}

		public Double getPrice()
		{
			return price;
		}

		public void setPrice(Double price)
		{
			this.price = price;
		}

		public Double getWeight()
		{
			return weight;
		}

		public void setWeight(Double weight)
		{
			this.weight = weight;
		}

		public String getCity()
		{
			return city;
		}

		public void setCity(String city)
		{
			this.city = city;
		}

		public String getDescription()
		{
			return description;
		}

		public void setDescription(String description)
		{
			this.description = description;
		}

		public List<Labels> getLabels()
		{
			return labels;
		}

		public void setLabels(List<Labels> labels)
		{
			this.labels = labels;
		}
	}

	private static enum Labels
	{
		SUPER1, SUPER2, SUPER3, SUPER4, SUPER5;
	}

	/****************************************************************************************************************/
	private static final ObjectMapper objectMapper = new ObjectMapper();

	private static class Movies
	{
		@Field
		private Long id;
		@Field("title")
		private String title;
		@Field("year")
		private Integer year;
		@Field("cast")
		private List<String> cast;
		@Field("genres")
		private List<String> genres;

		private static List<Movies> getMovies()
		{
			List<Movies> movies = new ArrayList<Movies>();
			try
			{
				movies = objectMapper.readValue(new File("C:\\Users\\USER\\IdeaProjects\\myTutorial\\solj-test\\src\\main\\resources\\movies_full.json"),
						new TypeReference<List<Movies>>()
						{
						});
			}
			catch (IOException exception)
			{
				System.out.println("Can't read data due to - " + exception.getMessage());
			}
			return movies;
		}

		public String getTitle()
		{
			return title;
		}

		public void setTitle(String title)
		{
			this.title = title;
		}

		public Integer getYear()
		{
			return year;
		}

		public void setYear(Integer year)
		{
			this.year = year;
		}

		public List<String> getCast()
		{
			return cast;
		}

		public void setCast(List<String> cast)
		{
			this.cast = cast;
		}

		public List<String> getGenres()
		{
			return genres;
		}

		public void setGenres(List<String> genres)
		{
			this.genres = genres;
		}
	}

	private static class ProductsV1
	{
		@Field
		private Long id;
		@Field
		private String sku;
		@Field
		private String name;
		@Field
		private String type;
		@Field
		private Double price;
		@Field
		private Long upc;
		@Field
		private List<Category> category;
		@Field
		private Integer shipping;
		@Field
		private String description;
		@Field
		private String manufacturer;
		@Field
		private String model;
		@Field
		private String url;
		@Field
		private String image;

		public String getSku()
		{
			return sku;
		}

		public void setSku(String sku)
		{
			this.sku = sku;
		}

		public String getName()
		{
			return name;
		}

		public void setName(String name)
		{
			this.name = name;
		}

		public String getType()
		{
			return type;
		}

		public void setType(String type)
		{
			this.type = type;
		}

		public Double getPrice()
		{
			return price;
		}

		public void setPrice(Double price)
		{
			this.price = price;
		}

		public Long getUpc()
		{
			return upc;
		}

		public void setUpc(Long upc)
		{
			this.upc = upc;
		}

		public List<Category> getCategory()
		{
			return category;
		}

		public void setCategory(List<Category> category)
		{
			this.category = category;
		}

		public Integer getShipping()
		{
			return shipping;
		}

		public void setShipping(Integer shipping)
		{
			this.shipping = shipping;
		}

		public String getDescription()
		{
			return description;
		}

		public void setDescription(String description)
		{
			this.description = description;
		}

		public String getManufacturer()
		{
			return manufacturer;
		}

		public void setManufacturer(String manufacturer)
		{
			this.manufacturer = manufacturer;
		}

		public String getModel()
		{
			return model;
		}

		public void setModel(String model)
		{
			this.model = model;
		}

		public String getUrl()
		{
			return url;
		}

		public void setUrl(String url)
		{
			this.url = url;
		}

		public String getImage()
		{
			return image;
		}

		public void setImage(String image)
		{
			this.image = image;
		}

		private static List<ProductsV1> getProductsV1()
		{
			List<ProductsV1> products = new ArrayList<ProductsV1>();
			try
			{
				products = objectMapper.readValue(new File("C:\\Users\\USER\\IdeaProjects\\myTutorial\\solj-test\\src\\main\\resources\\products.json"),
						new TypeReference<List<ProductsV1>>()
						{
						});
			}
			catch (IOException exception)
			{
				System.out.println("Can't read data due to - " + exception.getMessage());
			}
			return products;
		}

		private static class Category
		{
			private String id;
			private String name;

			public String getId()
			{
				return id;
			}

			public void setId(String id)
			{
				this.id = id;
			}

			public String getName()
			{
				return name;
			}

			public void setName(String name)
			{
				this.name = name;
			}

			@Override
			public String toString()
			{
				return name;
			}
		}
	}


	@JsonIgnoreProperties(ignoreUnknown = true)
	@JsonDeserialize(using = MovieV1Desirializer.class)
	private static class MovieV1
	{
		@Field("id")
		private String id;
		@Field("name_s")
		private String name;
		@Field("posterUrl_s")
		private String posterUrl;
		@Field("year_i")
		private Integer year;
		@Field("duration_i")
		private Integer duration;
		@Field("genre_ss")
		private List<String> genre;
		@Field("ratingValue_d")
		private Double ratingValue;
		@Field("ratingCount_i")
		private Integer ratingCount;
		@Field("director_txt")
		private String director;
		@Field("cast_txt")
		private List<String> cast;
		@Field("description_t")
		private String description;

		private static List<MovieV1> getMoviesV1(int percents)
		{
			List<MovieV1> list = new ArrayList<>(65000);
			File[] files = new File("C:\\Users\\USER\\Downloads\\datasets\\movies_dataset_2\\international-movies-json\\international-movies-json")
					.listFiles();
			if (percents != 100)
			{
				files = Arrays.copyOfRange(files, 0, files.length * percents / 100 - 1);
			}
			for (File file : files)
			{
				try
				{
					list.addAll((List<MovieV1>) objectMapper
							.readValue(file,
									new TypeReference<List<MovieV1>>()
									{
									}));
				}
				catch (IOException exception)
				{
					System.out.println("Can't parse JSON. " + exception.getMessage());
				}
			}
			return list;
		}

		public Integer getRatingCount()
		{
			return ratingCount;
		}

		public void setRatingCount(Integer ratingCount)
		{
			this.ratingCount = ratingCount;
		}

		public String getId()
		{
			return id;
		}

		public void setId(String id)
		{
			this.id = id;
		}

		public String getName()
		{
			return name;
		}

		public void setName(String name)
		{
			this.name = name;
		}

		public String getPosterUrl()
		{
			return posterUrl;
		}

		public void setPosterUrl(String posterUrl)
		{
			this.posterUrl = posterUrl;
		}

		public Integer getYear()
		{
			return year;
		}

		public void setYear(Integer year)
		{
			this.year = year;
		}

		public Integer getDuration()
		{
			return duration;
		}

		public void setDuration(Integer duration)
		{
			this.duration = duration;
		}

		public List<String> getGenre()
		{
			return genre;
		}

		public void setGenre(List<String> genre)
		{
			this.genre = genre;
		}

		public Double getRatingValue()
		{
			return ratingValue;
		}

		public void setRatingValue(Double ratingValue)
		{
			this.ratingValue = ratingValue;
		}

		public String getDirector()
		{
			return director;
		}

		public void setDirector(String director)
		{
			this.director = director;
		}

		public List<String> getCast()
		{
			return cast;
		}

		public void setCast(List<String> cast)
		{
			this.cast = cast;
		}

		public String getDescription()
		{
			return description;
		}

		public void setDescription(String description)
		{
			this.description = description;
		}
	}

	private static class MovieV1Desirializer extends StdNodeBasedDeserializer<MovieV1>
	{

		protected MovieV1Desirializer()
		{
			this((JavaType) null);
		}

		protected MovieV1Desirializer(JavaType targetType)
		{
			super(targetType);
		}

		protected MovieV1Desirializer(Class<MovieV1> targetType)
		{
			super(targetType);
		}

		protected MovieV1Desirializer(StdNodeBasedDeserializer<?> src)
		{
			super(src);
		}

		@Override
		public MovieV1 convert(JsonNode jsonNode, DeserializationContext deserializationContext) throws IOException
		{
			MovieV1 movie = new MovieV1();
			if (jsonNode.get("_id") != null)
				movie.setId(jsonNode.get("_id").asText());
			if (jsonNode.get("name") != null)
				movie.setName(jsonNode.get("name").asText());
			if (jsonNode.get("poster_url") != null)
				movie.setPosterUrl(jsonNode.get("poster_url").asText());
			String year = "";
			if (jsonNode.get("year") != null && (year = jsonNode.get("year").asText().replaceAll("\\D", "")).length() != 0)
			{
				movie.setYear(Integer.valueOf(year));
			}
			String duration = "";
			if (jsonNode.get("runtime") != null && (duration = jsonNode.get("runtime").asText().replaceAll("\\D", "")).length() != 0)
			{
				movie.setDuration(Integer.valueOf(duration));
			}
			if (jsonNode.get("genre") != null)
			{
				ArrayList<String> genres = new ArrayList<>();
				for (JsonNode g : jsonNode.get("genre"))
				{
					genres.add(g.asText());
				}
				movie.setGenre(genres);
			}
			if (jsonNode.get("ratingValue") != null)
				movie.setRatingValue(jsonNode.get("ratingValue").asDouble());
			if (jsonNode.get("summary_text") != null)
				movie.setDescription(jsonNode.get("summary_text").asText());
			if (jsonNode.get("ratingCount") != null && jsonNode.get("ratingCount").asText().replaceAll("\\D+", "").length() != 0)
				movie.setRatingCount(Integer.valueOf(jsonNode.get("ratingCount").asText().replaceAll("\\D+", "")));
			if (jsonNode.get("director") != null && jsonNode.get("director").get("name") != null)
				movie.setDirector(jsonNode.get("director").get("name").asText());
			if (jsonNode.get("cast") != null)
			{
				ArrayList<String> cast = new ArrayList<>();
				for (JsonNode c : jsonNode.get("cast"))
				{
					if (c.get("name") != null)
						cast.add(c.get("name").asText());
				}
				movie.setCast(cast);
			}
			return movie;
		}

	}




}
