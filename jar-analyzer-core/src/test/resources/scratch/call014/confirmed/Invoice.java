package demo;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;

/** CONFIRMED tier: @Entity (no @Table — JPA default table name = INVOICE), resolved across hops. */
@Entity
public class Invoice {}
