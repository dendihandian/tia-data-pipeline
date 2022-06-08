#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE TABLE IF NOT EXISTS public.posts (
    id INT PRIMARY KEY NOT NULL,
    date_gmt TIMESTAMP NOT NULL,
    modified_gmt TIMESTAMP NOT NULL,
    title VARCHAR NOT NULL,
    slug VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    link VARCHAR NOT NULL,
    content TEXT NOT NULL,
    vsitems JSON NULL,
    live_items JSON NULL,
    excerpt TEXT NOT NULL,
    author JSON NULL,
    editor VARCHAR NOT NULL,
    comment_status VARCHAR NOT NULL,
    comments_count INT NOT NULL,
    comments JSON NULL,
    featured_image JSON NULL,
    post_images JSON NULL,
    seo JSON NULL,
    categories JSON NULL,
    tags JSON NULL,
    companies JSON NULL,
    is_sponsored BOOLEAN NOT NULL,
    sponsor JSON NULL,
    is_partnership BOOLEAN NOT NULL,
    external_scripts JSON NULL,
    show_ads BOOLEAN NOT NULL,
    is_subscriber_exclusive BOOLEAN NOT NULL,
    is_paywalled BOOLEAN NOT NULL,
    is_inappbrowser BOOLEAN NOT NULL,
    read_time INT NOT NULL,
    word_count INT NULL
  );

  CREATE TABLE IF NOT EXISTS public.comments (
    id INT PRIMARY KEY NOT NULL,
    post INT NOT NULL,
    post_title VARCHAR NOT NULL,
    post_link VARCHAR NOT NULL,
    post_featured_image JSON NULL,
    post_comments_count INT NOT NULL,
    content TEXT NOT NULL,
    excerpt TEXT NOT NULL,
    status VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    parent INT NULL,
    author JSON NOT NULL,
    date_gmt TIMESTAMP NOT NULL,
    can_edit BOOLEAN NOT NULL,
    editable_until TIMESTAMP NULL,
    children JSON NULL,
    word_count INT NULL
  );
EOSQL
