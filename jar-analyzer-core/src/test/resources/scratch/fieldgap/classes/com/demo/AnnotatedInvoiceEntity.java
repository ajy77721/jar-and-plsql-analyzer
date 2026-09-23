package com.demo;

import org.springframework.data.mongodb.core.mapping.Document;

/** CALL-004: @Document-annotated entity — the CONFIRMED collection ground truth. */
@Document(collection = "invoices")
public class AnnotatedInvoiceEntity {}
