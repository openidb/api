import { z } from "@hono/zod-openapi";
import { PaginationQuery, SourceSchema } from "./common";

export const AuthorListQuery = PaginationQuery.extend({
  search: z.string().max(500).optional().openapi({ example: "ابن تيمية" }),
});

export const AuthorIdParam = z.object({
  id: z.string().max(50).openapi({ example: "123", description: "Author ID" }),
});

export const AuthorSummary = z.object({
  id: z.string(),
  nameArabic: z.string(),
  nameLatin: z.string().nullable(),
  deathDateHijri: z.string().nullable(),
  deathDateGregorian: z.string().nullable(),
  booksCount: z.number(),
});

export const AuthorListResponse = z.object({
  authors: z.array(AuthorSummary),
  total: z.number(),
  limit: z.number(),
  offset: z.number(),
  _sources: z.array(SourceSchema),
}).openapi("AuthorList");

export const AuthorDetailResponse = z.object({
  author: z.object({
    id: z.string(),
    nameArabic: z.string(),
    nameLatin: z.string().nullable(),
    kunya: z.string().nullable(),
    nasab: z.string().nullable(),
    nisba: z.string().nullable(),
    laqab: z.string().nullable(),
    birthDateHijri: z.string().nullable(),
    deathDateHijri: z.string().nullable(),
    birthDateGregorian: z.string().nullable(),
    deathDateGregorian: z.string().nullable(),
    biography: z.string().nullable(),
    biographySource: z.string().nullable(),
    books: z.array(z.object({
      id: z.string(),
      titleArabic: z.string(),
      titleLatin: z.string().nullable(),
    })),
  }),
  _sources: z.array(SourceSchema),
}).openapi("AuthorDetail");
