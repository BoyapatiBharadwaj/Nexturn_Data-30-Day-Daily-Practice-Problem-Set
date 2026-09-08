object Day2 {

  def main(args: Array[String]): Unit = {

    println("========================================")
    println("       DAY 2 - SCALA COLLECTIONS")
    println("========================================")


    // =====================================================
    // 1. MAP
    // =====================================================

    val sales = List(100, 200, 150, 300, 250)

    println("\n----- 1. MAP -----")

    val salesWithTax = sales.map(amount => amount * 1.18)

    println(s"Original sales: $sales")
    println(s"Sales with 18% tax: $salesWithTax")


    // =====================================================
    // 2. FILTER
    // =====================================================

    println("\n----- 2. FILTER -----")

    val highValueSales = sales.filter(amount => amount >= 200)

    println(s"Original sales: $sales")
    println(s"Sales >= 200: $highValueSales")


    // =====================================================
    // 3. FLATMAP
    // =====================================================

    println("\n----- 3. FLATMAP -----")

    val productLists = List(
      List("Laptop", "Mouse"),
      List("Keyboard"),
      List("Monitor", "USB Cable")
    )

    val allProducts = productLists.flatMap(products => products)

    println(s"Original nested list: $productLists")
    println(s"Flattened products: $allProducts")


    // =====================================================
    // 4. REDUCE
    // =====================================================

    println("\n----- 4. REDUCE -----")

    val totalSales = sales.reduce((a, b) => a + b)

    println(s"Sales: $sales")
    println(s"Total sales: $totalSales")


    // =====================================================
    // 5. VECTOR - INDEXED CUSTOMER RECORDS
    // =====================================================

    println("\n----- 5. VECTOR -----")

    val customers = Vector(
      Customer(1, "Rahul"),
      Customer(2, "Priya"),
      Customer(3, "Arun"),
      Customer(4, "Sneha")
    )

    println(s"Customers: $customers")

    println(s"Customer at index 0: ${customers(0)}")
    println(s"Customer at index 2: ${customers(2)}")


    // =====================================================
    // 6. MAP - PRODUCT QUANTITY
    // =====================================================

    println("\n----- 6. MAP - PRODUCT QUANTITY -----")

    val productQuantities = Map(
      "Laptop" -> 5,
      "Mouse" -> 20,
      "Keyboard" -> 10,
      "Monitor" -> 8
    )

    println(s"Product quantities: $productQuantities")

    println(s"Laptop quantity: ${productQuantities("Laptop")}")
    println(s"Mouse quantity: ${productQuantities("Mouse")}")


    // =====================================================
    // 7. MAP - PRODUCT PRICES
    // =====================================================

    println("\n----- 7. MAP - PRODUCT PRICES -----")

    val productPrices = Map(
      "Laptop" -> 60000.0,
      "Mouse" -> 800.0,
      "Keyboard" -> 1500.0,
      "Monitor" -> 12000.0
    )

    println(s"Product prices: $productPrices")

    println(s"Laptop price: ${productPrices("Laptop")}")
    println(s"Mouse price: ${productPrices("Mouse")}")


    // =====================================================
    // 8. FOR-COMPREHENSION
    // =====================================================

    println("\n----- 8. FOR-COMPREHENSION -----")

    val orders = List(
      Order(101, 1, "Laptop", 1),
      Order(102, 2, "Mouse", 3),
      Order(103, 3, "Keyboard", 2),
      Order(104, 1, "Monitor", 1),
      Order(105, 4, "Mouse", 2)
    )

    val customerOrders = for {
      customer <- customers
      order <- orders
      if customer.id == order.customerId
    } yield {
      s"${customer.name} ordered ${order.quantity} ${order.product}"
    }

    customerOrders.foreach(println)


    // =====================================================
    // 9. DAILY SALES SUMMARY
    // =====================================================

    println("\n========================================")
    println("         DAILY SALES SUMMARY")
    println("========================================")

    val salesDetails = for {
      order <- orders
      price <- productPrices.get(order.product)
      customer <- customers.find(_.id == order.customerId)
    } yield {

      val revenue = price * order.quantity

      SalesRecord(
        order.orderId,
        customer.name,
        order.product,
        order.quantity,
        price,
        revenue
      )
    }

    salesDetails.foreach { sale =>
      println(
        f"Order: ${sale.orderId}%-4d " +
        f"Customer: ${sale.customerName}%-6s " +
        f"Product: ${sale.product}%-9s " +
        f"Qty: ${sale.quantity}%2d " +
        f"Revenue: ₹${sale.revenue}%.2f"
      )
    }


    // =====================================================
    // 10. TOTAL DAILY REVENUE
    // =====================================================

    val dailyRevenue =
      salesDetails.map(_.revenue).sum

    println("\n----- TOTAL DAILY REVENUE -----")

    println(f"Total revenue: ₹$dailyRevenue%.2f")


    // =====================================================
    // 11. TOTAL QUANTITY SOLD
    // =====================================================

    val totalQuantity =
      salesDetails.map(_.quantity).sum

    println("\n----- TOTAL QUANTITY SOLD -----")

    println(s"Total products sold: $totalQuantity")


    // =====================================================
    // 12. HIGH VALUE ORDERS
    // =====================================================

    val highValueOrders =
      salesDetails.filter(_.revenue >= 10000)

    println("\n----- HIGH VALUE ORDERS -----")

    highValueOrders.foreach { sale =>
      println(
        f"${sale.customerName}%-6s " +
        f"${sale.product}%-9s " +
        f"₹${sale.revenue}%.2f"
      )
    }


    // =====================================================
    // 13. PRODUCT-WISE QUANTITY SUMMARY
    // =====================================================

    val quantityByProduct =
      salesDetails
        .groupBy(_.product)
        .map {
          case (product, records) =>
            product -> records.map(_.quantity).sum
        }

    println("\n----- PRODUCT-WISE QUANTITY -----")

    quantityByProduct.foreach {
      case (product, quantity) =>
        println(s"$product -> $quantity")
    }


    // =====================================================
    // 14. PRODUCT-WISE REVENUE SUMMARY
    // =====================================================

    val revenueByProduct =
      salesDetails
        .groupBy(_.product)
        .map {
          case (product, records) =>
            product -> records.map(_.revenue).sum
        }

    println("\n----- PRODUCT-WISE REVENUE -----")

    revenueByProduct.foreach {
      case (product, revenue) =>
        println(f"$product%-10s -> ₹$revenue%.2f")
    }


    // =====================================================
    // 15. TOP ORDER
    // =====================================================

    val highestOrder =
      salesDetails.maxBy(_.revenue)

    println("\n----- HIGHEST VALUE ORDER -----")

    println(
      f"${highestOrder.customerName} placed the highest order " +
      f"for ${highestOrder.product}: ₹${highestOrder.revenue}%.2f"
    )


    println("\n========================================")
    println("           DAY 2 COMPLETED")
    println("========================================")
  }
}


// =========================================================
// CUSTOMER
// =========================================================

case class Customer(
  id: Int,
  name: String
)


// =========================================================
// ORDER
// =========================================================

case class Order(
  orderId: Int,
  customerId: Int,
  product: String,
  quantity: Int
)


// =========================================================
// SALES RECORD
// =========================================================

case class SalesRecord(
  orderId: Int,
  customerName: String,
  product: String,
  quantity: Int,
  price: Double,
  revenue: Double
)
