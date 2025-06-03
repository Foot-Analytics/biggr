#
# General utils for harmonised data reading ----
#


#Find the related subjects that are contained given the graph, the subject, the relation between the parent and the subjects the type of the subjects (implementation of + operator of sparql property path)

find_related_subjects_per_type <- function(buildingsRdf, Subjects, relation, relatedSubjectsType) {
  # Lista para almacenar los sujetos encontrados
  related_subjects <- c(Subjects)
  
  # Inicializar la lista de sujetos a explorar
  to_explore <- c(Subjects)
  
  # While there are subjects to explore
  while (length(to_explore) > 0) {
    # Extract firs item of the list
    current_subject <- to_explore[1]
    to_explore <- to_explore[-1]
    
    # Buscar los sujetos relacionados a través de "relatedTo"
    result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),paste0("SELECT ?related
           WHERE {
             <",current_subject,">", relation," ?related .
             ?related a ",relatedSubjectsType,"
    }"))))
    
    # suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    #   set_query_prefixes_v2(namespaces),paste0("SELECT ?subject ?related
    #        WHERE {
    #         FILTER (?subject = <",current_subject,"> ).
    #         ?subject ", relation," ?related .
    #         ?related a ",relatedSubjectsType,"
    # }"))))
    
    # Ejecutar la consulta para encontrar sujetos relacionados
    # result <- rdf_query(g, query)
    
    # If there are new subjects, add them to the to_explore list
    if (nrow(result) > 0) {
      new_subjects <- result$related
      new_subjects <- new_subjects[!new_subjects %in% related_subjects]  # Evitar duplicados
      
      if (length(new_subjects) > 0) {
        related_subjects <- c(related_subjects, new_subjects)
        to_explore <- c(to_explore, new_subjects)
      }
    }
  }
  
  return(related_subjects[!(related_subjects %in% Subjects)])
}

find_all_related_subjects <- function(buildingsRdf, Subjects, relation) {
  # Lista para almacenar los sujetos encontrados
  related_subjects <- c(Subjects)
  
  # Inicializar la lista de sujetos a explorar
  to_explore <- c(Subjects)
  
  # While there are subjects to explore
  while (length(to_explore) > 0) {
    # Extract firs item of the list
    current_subject <- to_explore[1]
    to_explore <- to_explore[-1]
    
    # Buscar los sujetos relacionados a través de "relatedTo"
    result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),paste0("SELECT ?related
           WHERE {
             <",current_subject,">", relation," ?related .
    }"))))
    
    # suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    #   set_query_prefixes_v2(namespaces),paste0("SELECT ?subject ?related
    #        WHERE {
    #         FILTER (?subject = <",current_subject,"> ).
    #         ?subject ", relation," ?related .
    #         ?related a ",relatedSubjectsType,"
    # }"))))
    
    # Ejecutar la consulta para encontrar sujetos relacionados
    # result <- rdf_query(g, query)
    
    # If there are new subjects, add them to the to_explore list
    if (nrow(result) > 0) {
      new_subjects <- result$related
      new_subjects <- new_subjects[!new_subjects %in% related_subjects]  # Evitar duplicados
      
      if (length(new_subjects) > 0) {
        related_subjects <- c(related_subjects, new_subjects)
        to_explore <- c(to_explore, new_subjects)
      }
    }
  }
  
  return(related_subjects[!(related_subjects %in% Subjects)])
}

#Set the prefixes of all namespaces for a sparql query
set_query_prefixes_v2 <- function(namespaces){
  query_prefixes <- paste0(paste0(mapply(function(i){
    sprintf('PREFIX %s: <%s>', names(bigg_namespaces_v2)[i],
            bigg_namespaces_v2[i])},
    1:length(bigg_namespaces_v2))), collapse = "\n ")
  return(query_prefixes)
}



upload_covid_lockdown_inference_v2 <- function(buildingSubject, settings, affectedDates) {
  if (mongo_check("", settings)) {
    write("Loading the COVID lockdowns estimations to Mongo", stderr())
    mongo_conn("BuildingsInference", settings)$replace(
      query = sprintf('{"BuildingSubject": "%s"}', buildingSubject),
      update = jsonlite::toJSON(
        c(
          list('BuildingSubject' = buildingSubject),
          list('_updated' = biggr::format_iso_8601z(lubridate::with_tz(Sys.time(), "UTC"))),
          list('COVIDLockdownAffectance' = as.list(as.character(affectedDates)))
        ),
        na = 'null',
        auto_unbox = T
      ),
      upsert = TRUE
    )
  }
}

get_covid_lockdown_inference_v2 <- function(buildingSubject, settings) {
  if (mongo_check("", settings)) {
    write("Loading the COVID lockdowns estimations to Mongo", stderr())
    affectedDates <- mongo_conn("BuildingsInference", settings)$find(sprintf('{"BuildingSubject": "%s"}', buildingSubject))
    if (nrow(affectedDates) > 0) {
      return(list("exists" = TRUE, "dates" = as.Date(unlist(affectedDates$COVIDLockdownAffectance))))
    }
    return(list("exists" = FALSE, "dates" = NULL))
  }
}


#' Get timezone of a building
#' 
#' This function get from a BIGG_v2-harmonised dataset the timezone of a list of buildings.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param buildingSubjects <array> of URIs of the building subjects.
#' @return named <array> with the timezone for each building. 
#' The format of this time zones are defined by
#' the IANA Time Zone Database (https://www.iana.org/time-zones).

get_tz_building_v2 <- function(buildingsRdf, buildingSubjects){
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?b ?tz
    WHERE {
      ?b a s4bldg:Building .
      FILTER ( ?b IN (<',paste(buildingSubjects,collapse='>,<'),'>) ) .
      ?b vcard:hasAddress ?l .
      ?l bigg:addressTimeZone ?tz .
    }')))
  return( if(length(metadata_df)>0) {
    setNames(as.character(metadata_df$tz),nm=as.character(metadata_df$b))
  } else {NULL} )
}


get_tz_buildingSpace_v2 <- function(buildingsRdf, buildingSpace){
  metadata_df <- tibble(bs = character(0), tz = character(0))
  bind_rows(metadata_df,suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?bs ?tz
    WHERE {
      ?bs a s4bldg:BuildingSpace .
      FILTER ( ?bs IN (<',paste(buildingSpace,collapse='>,<'),'>) ) .
      ?bs vcard:hasAddress ?l .
      ?l bigg:addressTimeZone ?tz .
    }'))))
  if (!nrow(metadata_df>0)) {
    building_tz <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?b ?tz
    WHERE {
      ?b a s4bldg:Building .
      ?b vcard:hasAddress ?l .
      ?l bigg:addressTimeZone ?tz .
    }')))
    if (nrow(building_tz)>0) {
      for(building in building_tz$b) {
        bsp <- get_all_building_subspaces_v2(buildingsRdf,building)
        if (buildingSpace %in% bsp[[building]]) {
          metadata_df <- metadata_df %>% bind_rows(tibble(bs=buildingSpace, tz=building_tz[building_tz$b==building]$tz))
        }}
    } else {
      buildingspace_tz <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
        set_query_prefixes_v2(namespaces),
        '
        SELECT ?bs ?tz
        WHERE {
          ?bs a s4bldg:BuildingSpace .
          ?bs vcard:hasAddress ?l .
          ?l bigg:addressTimeZone ?tz .
        }')))
      if (nrow(buildingspace_tz)>0) {
        for(buildingspace in buildingspace_tz$bs) {
          bsp <- get_all_buldingspace_subspaces_v2(buildingsRdf,buildingspace)
          if (buildingSpace %in% bsp[[buildingspace]]) {
            metadata_df <- metadata_df %>% bind_rows(tibble(bs=buildingSpace, tz=buildingspace_tz[buildingspace_tz$bs==buildingSpace]$tz))
          }}
        if (nrow(metadata_df)>1) {
          list_subspaces <- get_all_buldingspace_subspaces_v2(buildingsRdf,metadata_df$bs)
          for (buildingspace in names(list_subspaces)) {
            if (count(metadata_df$bs %in% list_subspaces[[buildingspace]])>0){
              metadata_df <- metadata_df[metadata_df$bs!=buildingspace,]
            }
          }
        }
      }
    }
  }
  
  return( if(length(metadata_df)>0) {
    setNames(as.character(metadata_df$tz),nm=as.character(metadata_df$bs))
  } else {NULL} )
}


get_tz_measurement_v2 <- function(buildingsRdf, measurementId){
  measurement_building_system <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
      SELECT ?bs ?dev ?meas
      WHERE {
        ?bs s4syst:hasSubSystem ?dev .
        ?dev a saref:Device .
        ?dev saref:makesMeasurement ?meas .
        ?meas bigg:hash ?hash
        FILTER regex(str(?hash), "',measurementId,'")
      }')))$bs
  buildings <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
      SELECT ?b
      WHERE {
        ?b a s4bldg:Building
      }')))$b
  for(building in buildings){
    deployments <- get_building_deployments_by_buildingspace_v2(buildingsRdf,Building)[[Building]]$dp
    buildingsystems_aux <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?bs
      WHERE {
        ?bs a bigg:BuildingSystem .
        ?bs ssn:hasDeployment ?dp .
        FILTER ( ?dp IN (<',paste(deployments,collapse='>,<'),'>) ) .
      }')))
    buildingsystems <- c(as.character(buildingsystems_aux$bs),find_related_subjects_per_type(buildingsRdf, buildingsystems_aux$bs, "s4syst:hasSubSystem", "bigg:BuildingSystem"))
    systems <- find_related_subjects_per_type(buildingsRdf, buildingsystems, "s4syst:hasSubSystem", "s4syst:System")
    if (measurement_building_system %in% systems) {
      measurement_building <- building
    } else {
      measurement_building <- NULL
    }
  }
  metadata_df <- get_tz_building_v2(buildingsRdf, measurement_building)
 
  return( if(length(metadata_df)>0) {
    as.character(metadata_df[[measurement_building]])
  } else {"UTC"} )
}

get_all_building_subspaces_v2 <- function(buildingsRdf, buildingSubjects){
  
  # metadata_df <- suppressMessages(buildingsRdf_v2$query(paste0(    
  #   set_query_prefixes_v2(namespaces),
  #   '
  #   SELECT ?bs
  #       WHERE {
  #         ?b a s4bldg:Building .
  #         ?b geosp:sfContains+ ?bs.
  #         ?bs a s4bldg:BuildingSpace .
  #         
  #     }
  #   '))$bindings %>%
  #     as.list() %>%
  #     lapply(function(row) {
  #       tibble(
  #         bs = as.character(row[['bs']])
  #       )
  #     }) %>%
  #     bind_rows())
  metadata_df <- list()
  for (Building in buildingSubjects) {
    candidates <- find_related_subjects_per_type(buildingsRdf, Building, "geosp:sfContains", "s4bldg:BuildingSpace")
    metadata_df <- append(metadata_df,setNames(suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?bs
      WHERE {
        ?bs a s4bldg:BuildingSpace .
        FILTER ( ?bs IN (<',paste(candidates,collapse='>,<'),'>) ) .
      }'))), nm=Building))
  }
  
  return( 
    if(length(metadata_df)>0) {
     metadata_df
    } else { NULL } 
  )
}

get_all_buldingspace_subspaces_v2 <- function(buildingsRdf, buildingzoneSubjects){
  
  metadata_df <- list()
  for (Buildingzone in buildingzoneSubjects) {
    candidates <- find_related_subjects_per_type(buildingsRdf, Buildingzone, "geosp:sfContains", "s4bldg:BuildingSpace")
    metadata_df <- append(metadata_df,setNames(suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?bs
      WHERE {
        ?bs a s4bldg:BuildingSpace .
        FILTER ( ?bs IN (<',paste(candidates,collapse='>,<'),'>) ) .
      }'))), nm=Buildingzone))
  }
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}

get_building_undivisible_subspaces_v2 <- function(buildingsRdf, buildingSubjects){
  
  # metadata_df <- suppressMessages(buildingsRdf_v2$query(paste0(    
  #   set_query_prefixes_v2(namespaces),
  #   '
  #   SELECT ?bs
  #       WHERE {
  #         ?b a s4bldg:Building .
  #         ?b geosp:sfContains+ ?bs.
  #         FILTER NOT EXISTS {?bs geosp:sfContains ?subspace .}
  #         ?bs a s4bldg:BuildingSpace .
  # 
  #     }
  #   '))$bindings %>%
  #     as.list() %>%
  #     lapply(function(row) {
  #       tibble(
  #         bs = as.character(row[['bs']])
  #       )
  #     }) %>%
  #     bind_rows())
  metadata_df <- list()
  for (Building in buildingSubjects) {
    candidates <- find_related_subjects_per_type(buildingsRdf, buildingSubjects, "geosp:sfContains", "s4bldg:BuildingSpace")
    metadata_df <- append(metadata_df,setNames(suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?bs
    WHERE {
      ?bs a s4bldg:BuildingSpace .
      FILTER ( ?bs IN (<',paste(candidates,collapse='>,<'),'>) ) .
      OPTIONAL { ?bs geosp:sfContains ?subspace } .
      FILTER (!bound(?subspace))
    }'))), nm=Building))
    

  }
  
  # candidates <- find_related_subjects_per_type(buildingsRdf, buildingSubjects, "geosp:sfContains", "s4bldg:BuildingSpace")
  # metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
  #   set_query_prefixes_v2(namespaces),
  #   '
  #   SELECT ?bs
  #   WHERE {
  #     ?bs a s4bldg:BuildingSpace .
  #     FILTER ( ?bs IN (<',paste(candidates,collapse='>,<'),'>) ) .
  #     OPTIONAL { ?bs geosp:sfContains ?subspace } .
  #     FILTER (!bound(?subspace))
  #   }')))
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}

get_buldingspace_undivisible_subspaces_v2 <- function(buildingsRdf, buildingzoneSubjects){
  
  metadata_df <- list()
  for (Buildingzone in buildingzoneSubjects) {
    candidates <- find_related_subjects_per_type(buildingsRdf, Buildingzone, "geosp:sfContains", "s4bldg:BuildingSpace")
    metadata_df <- append(metadata_df,setNames(suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?bs
      WHERE {
        ?bs a s4bldg:BuildingSpace .
        FILTER ( ?bs IN (<',paste(candidates,collapse='>,<'),'>) ) .
        OPTIONAL { ?bs geosp:sfContains ?subspace } .
        FILTER (!bound(?subspace))
      }'))), nm=Buildingzone))
  }
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}

get_buldingspace_immediate_subspaces_list_v2 <- function(buildingsRdf, buildingzoneSubjects){
  
  metadata_df <- list()
  for (Buildingzone in buildingzoneSubjects) {
    metadata_df <- append(metadata_df,setNames(suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?subspace
      WHERE {
        ?bs a s4bldg:BuildingSpace .
        FILTER ( ?bs IN (<',paste(Buildingzone,collapse='>,<'),'>) ) .
        ?bs geosp:sfContains ?subspace .
      }'))), nm=Buildingzone))
  }
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}


#' Get gross floor area of a building
#' 
#' This function get from a BIGG_v2-harmonised dataset the gross floor area of a list of buildings.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param buildingSubjects <array> of URIs of the building subjects.
#' @return named <array> with the total gross floor area for each building.

get_area_building_v2 <- function(buildingsRdf, buildingSubjects){
  
  
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?b ?area
    WHERE {
      ?b a s4bldg:Building .
      FILTER ( ?b IN (<',paste(buildingSubjects,collapse='>,<'),'>) ) .
      ?b geosp:sfContains ?bz .
      ?bz bigg:hasArea ?a .
      ?a saref:relatesToProperty ?prop .
      FILTER regex(str(?prop),"GrossFloorArea")
      ?a saref:hasValue ?area .
    }')))
  if(length(metadata_df)>0) {
    metadata_df$area <- as.numeric(metadata_df$area)
    metadata_df <- metadata_df %>% group_by(b) %>% summarise(area=sum(area,na.rm=T))
    r <- setNames(ifelse(metadata_df$area>0,metadata_df$area,NA),nm=as.character(metadata_df$b))
  } else { r <- NULL }
  return(r)
}


#' Get gross floor area of a building
#' 
#' This function get from a BIGG_v2-harmonised dataset the gross floor area of a list of buildings.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param buildingSubjects <array> of URIs of the building subjects.
#' @return named <array> with the total gross floor area for each building.

get_area_buildingSpace_v2 <- function(buildingsRdf, buildingSpaces){
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?b ?area
    WHERE {
      ?b a s4bldg:BuildingSpace .
      FILTER ( ?b IN (<',paste(buildingSpaces,collapse='>,<'),'>) ) .
      ?b bigg:hasArea ?a .
      ?a saref:relatesToProperty ?prop .
      FILTER regex(str(?prop),"GrossFloorArea")
      ?a saref:hasValue ?area .
    }')))
  if(length(metadata_df)>0) {
    metadata_df$area <- as.numeric(metadata_df$area)
    metadata_df <- metadata_df %>% group_by(b) %>% summarise(area=sum(area,na.rm=T))
    r <- setNames(ifelse(metadata_df$area>0,metadata_df$area,NA),nm=as.character(metadata_df$b))
  } else { r <- NULL }
  return(r)
}


get_building_deployments_by_buildingspace_v2 <- function(buildingsRdf, buildingSubjects){
  
  metadata_df <- list()
  for (Building in buildingSubjects){
    
    buildingZones <- get_all_building_subspaces_v2(buildingsRdf, Building)[[Building]]
    # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
    metadata_df <- append(metadata_df,setNames(list(suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?bz ?dp
      WHERE {
        ?bz a s4bldg:BuildingSpace .
        FILTER ( ?bz IN (<',paste(buildingZones,collapse='>,<'),'>) ) .
        ?dp a s4agri:Deployment .
        ?dp s4agri:isDeployedAtSpace ?bz .
      }')))), nm=Building))
  }
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}


get_buildingspaces_deployments_v2 <- function(buildingsRdf, buildingSpaces){
  
  metadata_df <- list()
  for (BuildingSpace in buildingSpaces){
    
    # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
    metadata_df <- append(metadata_df,setNames(suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?dp
      WHERE {
        ?bz a s4bldg:BuildingSpace .
        FILTER ( ?bz IN (<',paste(BuildingSpace,collapse='>,<'),'>) ) .
        ?dp a s4agri:Deployment .
        ?dp s4agri:isDeployedAtSpace ?bz .
      }'))), nm=BuildingSpace))
  }
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}


get_building_systems_list_v2 <- function(buildingsRdf, buildingSubjects){
  
  metadata_df <- list()
  for (Building in buildingSubjects) {
    
    # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
    aux_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?bs ?bz
      WHERE {
        ?bs a s4syst:System .
        ?bs ssn:hasDeployment ?dp .
        ?dp s4agri:isDeployedAtSpace ?bz .
       
      }')))
  
  aux <- aux_df[(aux_df$bz %in% find_related_subjects_per_type(buildingsRdf, buildingSubjects, "geosp:sfContains", "s4bldg:BuildingSpace")),]
  
  # find_related_subjects_per_type(buildingsRdf, aux_df$bs, "s4syst:hasSubSystem", "bigg:BuildingSystem")
  
  metadata_df <- append(metadata_df, setNames(list(c(as.character(aux$bs),find_related_subjects_per_type(buildingsRdf, aux_df$bs, "s4syst:hasSubSystem", "s4syst:System"))),
                                              nm=Building))
  
  }
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}



get_building_deployments_names_v2 <- function(buildingsRdf, buildingSubjects){
  
  metadata_df <- list()
  for (Building in buildingSubjects) {
    
    # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
    aux_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?dp ?bz ?dpn
      WHERE {
        ?bs ssn:hasDeployment ?dp .
        ?dp bigg:deploymentType ?dpn .
        ?dp s4agri:isDeployedAtSpace ?bz .
       
      }')))
    
    aux <- aux_df[(aux_df$bz %in% find_related_subjects_per_type(buildingsRdf, buildingSubjects, "geosp:sfContains", "s4bldg:BuildingSpace")),]
    
    # find_related_subjects_per_type(buildingsRdf, aux_df$bs, "s4syst:hasSubSystem", "bigg:BuildingSystem")
    
    metadata_df <- append(metadata_df, setNames(list(as.character(aux$dpn)),
                                                nm=Building))
    
  }
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}



get_buildingspaces_systems_list_v2 <- function(buildingsRdf, buildingspaceSubjects){
  
  # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
  aux_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
      SELECT ?s ?bz
      WHERE {
        ?s a s4syst:System .
        ?s ssn:hasDeployment ?dp .
        ?dp s4agri:isDeployedAtSpace ?bz .
       
      }')))
  
  metadata_df <- list()
  for (buildingspace in buildingspaceSubjects) {
    aux <- c(aux_df[(aux_df$bz==buildingspace),]$s,find_related_subjects_per_type(buildingsRdf, aux_df[(aux_df$bz==buildingspace),]$s, "s4syst:hasSubSystem", "s4syst:System"))
  # aux <- aux_df[(aux_df$bz %in% find_related_subjects_per_type(buildingsRdf, buildingSubjects, "geosp:sfContains", "s4bldg:BuildingSpace")),]
  
  # find_related_subjects_per_type(buildingsRdf, aux_df$bs, "s4syst:hasSubSystem", "bigg:BuildingSystem")
  
    metadata_df <- append(metadata_df, setNames(list(as.character(aux)),
                                              nm=buildingspace))
  }
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}


get_all_buildingspaces_systems_v2 <- function(buildingsRdf, buildingspaceSubjects){
  
  # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
  aux_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
      SELECT ?bz
      WHERE {
        ?bz a s4bldg:BuildingSpace .
        }')))
  
  metadata_df <- tibble(buildingSpace=character(), system=character())
  for (i in 1:nrow(aux_df)) {
    metadata_df <- bind_rows(metadata_df, tibble(buildingSpace=rep(aux_df[i,]$bz,length(get_buildingspaces_systems_list_v2(buildingsRdf, aux_df[i,]$bz)[[ aux_df[i,]$bz]])),
                             system=get_buildingspaces_systems_list_v2(buildingsRdf, aux_df[i,]$bz)[[ aux_df[i,]$bz]]))
  }
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}

get_building_buildingsystems_v2 <- function(buildingsRdf, buildingSubjects){
  
  metadata_df <- list()
  for (Building in buildingSubjects) {
  
    # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
    aux_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?bs ?bz
      WHERE {
        ?bs a bigg:BuildingSystem .
        ?bs ssn:hasDeployment ?dp .
        ?dp s4agri:isDeployedAtSpace ?bz .
       
      }')))
      
    aux <- aux_df[(aux_df$bz %in% find_related_subjects_per_type(buildingsRdf, buildingSubjects, "geosp:sfContains", "s4bldg:BuildingSpace")),]
    
    # find_related_subjects_per_type(buildingsRdf, aux_df$bs, "s4syst:hasSubSystem", "bigg:BuildingSystem")
    
    metadata_df <- append(metadata_df, setNames(list(c(as.character(aux$bs),find_related_subjects_per_type(buildingsRdf, aux_df$bs, "s4syst:hasSubSystem", "bigg:BuildingSystem"))),
                                   nm=Building))
  
  }
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}



get_building_buildingsystems_systems_v2 <- function(buildingsRdf, buildingSubjects){
  
  metadata_df <- list()
  for (Building in buildingSubjects) {
    
    # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
    aux_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?bs ?bz
      WHERE {
        ?bs a bigg:BuildingSystem .
        ?bs ssn:hasDeployment ?dp .
        ?dp s4agri:isDeployedAtSpace ?bz .
       
      }')))
  
  aux <- aux_df[(aux_df$bz %in% find_related_subjects_per_type(buildingsRdf, buildingSubjects, "geosp:sfContains", "s4bldg:BuildingSpace")),]
  
  # find_related_subjects_per_type(buildingsRdf, aux_df$bs, "s4syst:hasSubSystem", "bigg:BuildingSystem")
  
  metadata_df <- append(metadata_df, setNames(list(c(as.character(aux$bs),find_related_subjects_per_type(buildingsRdf, aux_df$bs, "s4syst:hasSubSystem", "s4syst:System"))),
                                              nm=Building))
  }
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}


get_building_weather_station_systems_v2 <- function(buildingsRdf, buildingSubjects){
  
  metadata_df <- list()
  for (Building in buildingSubjects)
    
    # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
    aux_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?ws ?bz
      WHERE {
        ?ws a s4agri:WeatherStation .
        ?ws ssn:hasDeployment ?dp .
        ?dp s4agri:isDeployedAtSpace ?bz .
       
      }')))
  
  aux <- aux_df[(aux_df$bz %in% find_related_subjects_per_type(buildingsRdf, buildingSubjects, "geosp:sfContains", "s4bldg:BuildingSpace")),]
  
  # find_related_subjects_per_type(buildingsRdf, aux_df$bs, "s4syst:hasSubSystem", "bigg:BuildingSystem")
  
  metadata_df <- append(metadata_df, setNames(list(c(as.character(aux$ws),find_related_subjects_per_type(buildingsRdf, aux_df$ws, "s4syst:hasSubSystem", "s4syst:System"))),
                                              nm=Building))
  
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}


#' Get GEONAMES attributes from buildings
#' 
get_geonames_attr_from_buildings_v2 <- function(buildingsRdf, buildingSubjects){
  
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?buildingSubject ?provinceName ?countryCode
    WHERE {
        ?f a gn:feature .
        ?buildingSubject a s4bldg:Building .
        FILTER ( ?buildingSubject IN (<',paste(buildingSubjects,collapse='>,<'),'>) ) .
        ?f gn:parentADM2 ?fp .
        ?fp gn:featureCode ?fc .
        FILTER regex(str(?fc),"ADM2") .
        ?fp gn:name ?provinceName .
        ?fp gn:countryCode ?countryCode .
    }')))
  return( if(length(metadata_df)>0) {
    metadata_df
  } else {NULL} )
}



#' Get ISO 3166-2 Region Codes from buildings using Country Code and Province information
#' 
get_std_regioncode_from_buildings_v2 <- function(countryCode, Province){
  
  if (countryCode == "ES"){
    if (Province == "A Coruña " || Province == "La Coruña") {
      regionCode = "GA"
    } else if (Province == "Álava" || Province == "Araba") {
      regionCode = "PV"
    } else if (Province == "Albacete") {
      regionCode = "CM"
    } else if (Province == "Alicante" || Province == "Alacant") {
      regionCode = "VC"
    } else if (Province == "Almería") {
      regionCode = "AN"
    } else if (Province == "Asturias") {
      regionCode = "AS"
    } else if (Province == "Ávila") {
      regionCode = "CL"
    } else if (Province == "Badajoz") {
      regionCode = "EX"
    } else if (Province == "Barcelona") {
      regionCode = "CT"
    } else if (Province == "Bizkaia") {
      regionCode = "PV"
    } else if (Province == "Burgos") {
      regionCode = "CL"
    } else if (Province == "Cáceres") {
      regionCode = "EX"
    } else if (Province == "Cádiz") {
      regionCode = "AN"
    } else if (Province == "Cantabria") {
      regionCode = "CB"
    } else if (Province == "Castellón" || Province == "Castelló") {
      regionCode = "VC"
    } else if (Province == "Ciudad Real") {
      regionCode = "CM"
    } else if (Province == "Córdoba") {
      regionCode = "AN"
    } else if (Province == "Cuenca") {
      regionCode = "CM"
    } else if (Province == "Gipuzkoa") {
      regionCode = "PV"
    } else if (Province == "Girona" || Province == "Gerona") {
      regionCode = "CT"
    } else if (Province == "Granada") {
      regionCode = "AN"
    } else if (Province == "Guadalajara") {
      regionCode = "CM"
    } else if (Province == "Huelva") {
      regionCode = "AN"
    } else if (Province == "Huesca") {
      regionCode = "AR"
    } else if (Province == "Illes Balears" || Province == "Islas Baleares") {
      regionCode = "IB"
    } else if (Province == "Jaén") {
      regionCode = "AN"
    } else if (Province == "La Rioja") {
      regionCode = "RI"
    } else if (Province == "Las Palmas") {
      regionCode = "CN"
    } else if (Province == "León") {
      regionCode = "CL"
    } else if (Province == "Lleida" || Province == "Lérida") {
      regionCode = "CT"
    } else if (Province == "Lugo") {
      regionCode = "GA"
    } else if (Province == "Madrid") {
      regionCode = "MD"
    } else if (Province == "Málaga") {
      regionCode = "AN"
    } else if (Province == "Murcia") {
      regionCode = "MC"
    } else if (Province == "Navarra" || Province == "Nafarroa") {
      regionCode = "NC"
    }else if (Province == "	Ourense" || Province == "Orense") {
      regionCode = "GA"
    }else if (Province == "Palencia") {
      regionCode = "NC"
    }else if (Province == "Pontevedra") {
      regionCode = "GA"
    }else if (Province == "Salamanca") {
      regionCode = "CL"
    }else if (Province == "Santa Cruz de Tenerife") {
      regionCode = "CN"
    }else if (Province == "Segovia") {
      regionCode = "CL"
    }else if (Province == "Sevilla") {
      regionCode = "AN"
    }else if (Province == "Soria") {
      regionCode = "CL"
    }else if (Province == "Tarragona") {
      regionCode = "CT"
    }else if (Province == "Teruel") {
      regionCode = "AR"
    }else if (Province == "Toledo") {
      regionCode = "CM"
    }else if (Province == "Valencia" || Province == "València") {
      regionCode = "VC"
    }else if (Province == "Valladolid") {
      regionCode = "CL"
    }else if (Province == "Zamora") {
      regionCode = "CL"
    }else if (Province == "Zaragoza") {
      regionCode = "AR"
    } else {regionCode = NULL}
  } else {
    regionCode = NULL
  }
  return(regionCode)
}


#' Get namespaces of a building
#' 
#' This function get from a BIGG-harmonised dataset the namespaces of a list of buildings.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param buildingSubjects <array> of URIs of the building subjects.
#' @return named <array> with the namespace for each building.

get_building_namespaces_v2 <- function(buildingsRdf, buildingSubjects){
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?b
    WHERE {
      ?b a s4bldg:Building .
      FILTER ( ?b IN (<',paste(buildingSubjects,collapse='>,<'),'>) ) .
    }')))
  
  return( 
    if(length(metadata_df)>0) {
      setNames(mapply(function(x){
        paste0(strsplit(metadata_df$b,"#")[[x]][1],"#")},1:nrow(metadata_df)),
        nm=metadata_df$b)
    } else { NULL } 
  )
}

#' Get building subjects from a set of building identifier from organisation
#' 
#' This function get the building subjects based on a list of building identifier(s) from organisation.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param buildingIdsFromOrganization <array> of strings containing the building identifiers from the organisation.
#' @return <array> with the subject URI for each building.

get_buildings_subjects_v2 <- function(buildingsRdf, buildingIdsFromOrganization){
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?o ?b
    WHERE {
      ?p a bigg:Patrimony .
      ?p bigg:isManagedBy ?org .
      ?org bigg:idFromOrganization ?o .
      FILTER ( ?o IN ("',paste(buildingIdsFromOrganization,collapse='","'),'") ) .
      ?p geosp:sfContains ?b .
      ?b a s4bldg:Building
    }')))
  
  return( 
    if(length(metadata_df)>0) {
      setNames(as.character(metadata_df$b),nm=as.character(metadata_df$o))
    } else { NULL } 
  )
}

#' Calculate the building identifiers
#' 
#' This function calculates the building identifiers from a list of building subjects.
#'
#' @param buildingSubjects <array> of URIs of the building subjects.
#' @return 

get_building_identifiers_v2 <- function(buildingSubjects){
  return(
    setNames(gsub("http://|https://||#||\\.","",buildingSubjects),nm = buildingSubjects)
  )
}

get_buildingspace_identifiers_v2 <- function(buildingspaceSubjects){
  return(
    setNames(gsub("http://|https://||#||\\.","",buildingspaceSubjects),nm = buildingspaceSubjects)
  )
}

uri_to_identifier_v2 <- function(uris){
  return(
    setNames(gsub("http://|https://||#||\\.","",uris),nm = uris)
  )
}

#' Get all building subjects
#' 
#' This function get all building subjects available from a BIGG-harmonised dataset.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param buildingSubjects <array> of URIs of the building subjects.
#' @return <array> with the subject URI for each building.

get_all_buildings_list_v2 <- function(buildingsRdf){
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?b
    WHERE {
      ?b a s4bldg:Building .
    }')))
  return( if(length(metadata_df)>0) {as.character(metadata_df$b)} else {NULL} )
}

#' Check if exists an analytical model.
#' 
#' This function checks if certain analytical model subject exists in a BIGG-harmonised dataset.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param modelSubject <uri> of an analytical model.
#' @param namespaces named <array> that relates simple namespaces and complete.
#' ones.
#' @return <boolean> if the model exists.

exists_analytical_model_v2 <- function(buildingsRdf, modelSubject, namespaces){
  
  modelSubject <- namespace_integrator(modelSubject, namespaces)
  return(nrow(suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?m
    WHERE {
      ?m a bigg:AnalyticalModel .
      FILTER (?m = <',modelSubject,'>) .
    }'))))>0)
}

#' ISO 8601 period to natural text
#' 
#' This function converts an ISO 8601 period (e.g. PT1H30M) to natural text (in the example, "1 hour, 30 mins").
#'
#' @param x <string> defining the frequency to be converted. 
#' It must follow ISO 8601 format representing the time step. 
#' @param only_first <boolean> specifying if only the first 
#' timestep item should be converted.
#' @return <string> with the time step in natural text.

iso8601_period_to_text_v2 <- function(x,only_first=F){
  x <- lubridate::period(x)
  items <- c("year"=x@year,"month"=x@month,"day"=x@day,
             "hour"=x@hour,"min"=x@minute,
             "sec"=lubridate::second(x))
  text_items <- lapply(FUN = function(i){
    if(items[i]>0){
      paste(items[i], 
            if(items[i]>1){paste0(names(items)[i],"s")
            } else { names(items)[i] })
    }},1:length(items))
  text_items <- text_items[mapply(function(i)!is.null(i),text_items)]
  if(only_first)
    text_items <- text_items[1]
  return(do.call(function(...) paste(..., sep=", "), text_items))
}

#' ISO 8601 period to period
#' 
#' This function converts an ISO 8601 period (e.g. PT1H30M) to a period object.
#'
#' @param x <string> defining the frequency to be converted. 
#' It must follow ISO 8601 format representing the time step.
#' @return <period> according the format defined in lubridate's 'Period-class'.

iso8601_period_to_timedelta_v2 <- function(x){
  x <- lubridate::period(x)
  return(lubridate::years(x@year) + months(x@month) + lubridate::days(x@day) +
           lubridate::hours(x@hour) + lubridate::minutes(x@minute) + lubridate::seconds(lubridate::second(x)))
}

#
# Check data and analytics compliance ----
#

#' Get the metadata of all measurements by building
#'  
#' This function gets all the measurements available in a BIGG-harmonised dataset.
#' It also provides metadata with the time series characteristics of these measurements 
#' and their relation with building, building space, data provider and sensor concepts.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @return 

get_measurements_by_building_metadata_v2 <- function(buildingsRdf){
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(
    paste0(mapply(function(i){
      sprintf('PREFIX %s: <%s>', names(bigg_namespaces_v2)[i],
              bigg_namespaces_v2[i])},
      1:length(bigg_namespaces_v2))),
    '
    SELECT ?buildingSubject ?buildingSpace ?dataProvider ?sensorId
    WHERE {
      {
        SELECT ?buildingSubject ?buildingSpace ?dataProvider ?sensor
        WHERE {
          ?buildingSubject a bigg:Building .
          ?buildingSubject bigg:hasSpace ?buildingSpace .
          ?buildingSpace bigg:isObservedByDevice ?dataProvider .
          ?dataProvider bigg:hasSensor ?sensor .
        }
      }
      ?sensor bigg:hasMeasurement ?sensorId .
    }')))
  return(metadata_df)
}
# 
# check_measurements_by_building <- function(buildingsRdf, timeseriesObject, updateHadoopStatus=F){
#   
#   write("Checking the measurements and devices aggregators by building",stderr())
#   
#   # Get the measurements and device aggregators by building
#   measurements_metadata <- get_measurements_by_building_metadata_v2(buildingsRdf)
#   if(nrow(measurements_metadata)>0){
#     measurements_metadata$sensorId <- gsub(".*#","",measurements_metadata$sensorId,fixed = F)
#   }
#   dev_aggregator_metadata <- get_device_aggregators_metadata(buildingsRdf)
#   if(nrow(dev_aggregator_metadata)>0){
#   dev_aggregator_metadata$sensorIdsRelated <- lapply(FUN = function(x)unlist(x[,2]),
#          stringr::str_match_all(dev_aggregator_metadata$deviceAggregatorFormula, 
#            "<mi>\\s*(.*?)\\s*</mi>"))}
#   
#   # Check and summarise time series data from the related measurements
#   n_sensors <- nrow(measurements_metadata)
#   if(n_sensors>0){
#     sensors <- lapply(1:n_sensors, FUN = function(i){
#       sensorId <- measurements_metadata$sensorId[i]
#       if(updateHadoopStatus==T){
#         write(sprintf("reporter:status: READING %s (%s/%s)", sensorId, i, n_sensors), stderr())
#       } else {
#         write(sprintf("  Sensor %s/%s",i,n_sensors),stderr())
#       }
#       sensor_data <- suppressMessages(suppressWarnings(
#         get_sensor_v2(timeseriesObject, buildingsRdf, sensorId, 
#           tz=NULL, outputFrequency="P1Y", aggFunctions=NULL,
#           useEstimatedValues=F, integrateCost=T, integrateEmissions=T, 
#           transformToAggregatableMeasuredProperty=F, aggregatableMeasuredPropertyName=NULL, 
#           defaultFactorsByMeasuredProperty=NULL, obtainMetadata=T)
#         ))
#       if(is.null(sensor_data$timeseries)){
#         sensor_data$metadata$summary <- NA 
#       } else {
#         #colnames(sensor_data$timeseries)[2] <- "value"
#         sensor_data$metadata$summary <- list(sensor_data$timeseries)
#       }
#       sensor_data$metadata$containsData <- !is.null(sensor_data$timeseries)
#       sensor_data$metadata})
#     sensors_df <- do.call(rbind,sensors)
#     measurements_metadata <- measurements_metadata %>% left_join(sensors_df,by="sensorId")
#   }
#   
#   # Check which device aggregators can be calculated, or not.
#   if(nrow(dev_aggregator_metadata)>0){
#     dev_aggregator_metadata$existsDataFromSensors <- mapply(function(i){
#       all(measurements_metadata[
#             measurements_metadata$sensorId %in% 
#               dev_aggregator_metadata$sensorIdsRelated[[i]],
#           ]$containsData)
#     },1:nrow(dev_aggregator_metadata))
#     dev_aggregator_metadata$timeSeriesStart <- as.POSIXct(
#       mapply(FUN = function(i){
#         if (dev_aggregator_metadata$existsDataFromSensors[i]){
#           max(measurements_metadata[
#             measurements_metadata$sensorId %in%
#               dev_aggregator_metadata$sensorIdsRelated[[i]],
#           ]$timeSeriesStart)
#         } else {
#           NA
#         }
#       },1:nrow(dev_aggregator_metadata)),tz="UTC",
#       origin=as.POSIXct("1970-01-01 00:00:00",tz="UTC"))
#     dev_aggregator_metadata$timeSeriesEnd <- as.POSIXct(mapply(function(i){
#       if (dev_aggregator_metadata$existsDataFromSensors[i]){
#         min(measurements_metadata[
#           measurements_metadata$sensorId %in% 
#             dev_aggregator_metadata$sensorIdsRelated[[i]],
#         ]$timeSeriesEnd)
#       } else {
#         NA
#       }
#     },1:nrow(dev_aggregator_metadata)),tz="UTC",
#     origin=as.POSIXct("1970-01-01 00:00:00",tz="UTC"))
#     dev_aggregator_metadata$timeSeriesNumberOfDays <- 
#       ifelse(is.finite(dev_aggregator_metadata$timeSeriesEnd) & is.finite(dev_aggregator_metadata$timeSeriesStart),
#         lubridate::period_to_seconds(lubridate::as.period(
#           dev_aggregator_metadata$timeSeriesEnd - dev_aggregator_metadata$timeSeriesStart)) / (3600 * 24),
#       0)
#     dev_aggregator_metadata$aggregationCanBeCalculated <- 
#       dev_aggregator_metadata$existsDataFromSensors & 
#       dev_aggregator_metadata$timeSeriesStart <= dev_aggregator_metadata$timeSeriesEnd
#     dev_aggregator_metadata$aggregationCanBeCalculated <- ifelse(is.na(
#       dev_aggregator_metadata$aggregationCanBeCalculated), F, 
#       dev_aggregator_metadata$aggregationCanBeCalculated)
#   }
#   
#   return(
#     list("Measurements"=measurements_metadata,
#          "DeviceAggregators"=dev_aggregator_metadata))
# }
# 
# check_static_information_by_building <- function(buildingsRdf, updateHadoopStatus=F){
#   
#   write("Checking the static information by building",stderr())
#   
#   if(updateHadoopStatus==T){
#     write("reporter:status: READING Buildings", stderr())
#   }
#   
#   metadata <- data.frame(
#     buildingSubject = get_all_buildings_list(buildingsRdf))
#   # Building area 
#   areas <- get_area_building_v2(buildingsRdf,metadata$buildingSubject)
#   if(!is.null(areas)){
#     metadata <- metadata %>% full_join(
#         data.frame("buildingSubject" = names(areas),"area"=areas),
#         by="buildingSubject"
#     )
#   } else {
#     metadata$area <- NA
#   }
#   # Building related single EEM
#   eems <- get_building_eems(buildingsRdf)
#   if (!is.null(eems)){
#     metadata <- metadata %>% full_join(
#       eems %>% group_by(buildingSubject) %>% 
#         summarise(numberOfEEMs = length(unique(eemSubject[!is.na(eemSubject)]))),
#       by="buildingSubject"
#     )
#   } else {
#     metadata$numberOfEEMs <- 0
#   }
#   # Building related projects
#   projects <- get_eem_projects(buildingsRdf,metadata$buildingSubject)
#   if (!is.null(projects)){
#     metadata <- metadata %>% full_join(
#       projects %>% group_by(buildingSubject) %>% 
#         summarise(numberOfProjects = length(unique(eemProjectSubject[!is.na(eemProjectSubject)]))),
#       by="buildingSubject"
#     )
#   } else {
#     metadata$numberOfProjects <- 0
#   }
#   return(metadata)
# }
# 
# data_requirements_compliance_by_building <- function(buildingsRdf, timeseriesObject, settings, updateHadoopStatus=F){
#   
#   checkedMetadata <- check_measurements_by_building(buildingsRdf, timeseriesObject, updateHadoopStatus)
#   checkedMetadata$Building <- check_static_information_by_building(buildingsRdf, updateHadoopStatus)
#   
#   write("Checking the data requirements compliance by building",stderr())
#   
#   servicesRequirements <- settings$DataRequirementsForAnalyticalServices
#   if(updateHadoopStatus==T){
#     write("reporter:status: CALCULATING COMPLIANCE by buildings", stderr())
#   }
#   
#   checkedResults <- lapply(
#     unique(checkedMetadata$Building$buildingSubject),
#     function(buildingSubject){
#     
#     measurementsBuilding <- if(nrow(checkedMetadata$Measurements)>0){
#       checkedMetadata$Measurements[checkedMetadata$Measurements$buildingSubject==buildingSubject,]
#     } else {checkedMetadata$Measurements}
#     deviceAggregatorsBuilding <- if(nrow(checkedMetadata$DeviceAggregators)>0){
#       checkedMetadata$DeviceAggregators[checkedMetadata$DeviceAggregators$buildingSubject==buildingSubject,]
#     } else {checkedMetadata$DeviceAggregators}
#     staticsBuilding <- checkedMetadata$Building[checkedMetadata$Building$buildingSubject==buildingSubject,]
#     
#     services <- data.frame(
#       Name = servicesRequirements[,"Name"],
#       do.call(rbind,lapply(FUN = function(i){
#         service <- servicesRequirements[i,]
#         
#         # DeviceAggregator checking
#         DeviceAggregators_check <- if((length(service$DeviceAggregators$AllValid[[1]])>0 ||
#                                        length(service$DeviceAggregators$AnyValid[[1]])>0) &&
#                                       nrow(deviceAggregatorsBuilding)==0){
#           FALSE
#         } else if(length(service$DeviceAggregators$AllValid[[1]])>0 &&
#                                       length(service$DeviceAggregators$AnyValid[[1]])>0){
#           all(
#             all(mapply(FUN = function(x) nrow(deviceAggregatorsBuilding %>% filter(eval(parse(text=x))))>0, 
#                        service$DeviceAggregators$AllValid[[1]])),
#             any(mapply(function(x) nrow(deviceAggregatorsBuilding %>% filter(eval(parse(text=x))))>0, 
#                        service$DeviceAggregators$AnyValid[[1]]))
#           )
#         } else if(length(service$DeviceAggregators$AllValid[[1]])>0){
#           all(mapply(FUN = function(x) nrow(deviceAggregatorsBuilding %>% filter(eval(parse(text=x))))>0, 
#                      service$DeviceAggregators$AllValid[[1]]))
#         } else if(length(service$DeviceAggregators$AnyValid[[1]])>0){
#           any(mapply(FUN = function(x) nrow(deviceAggregatorsBuilding %>% filter(eval(parse(text=x))))>0, 
#                      service$DeviceAggregators$AnyValid[[1]]))
#         } else {
#           NA
#         }
#         
#         # Measurements checking
#         Measurements_check <- if((length(service$Measurements$AllValid[[1]])>0 ||
#                                   length(service$Measurements$AnyValid[[1]])>0) &&
#                                  nrow(measurementsBuilding)==0){
#           FALSE
#         } else if(length(service$Measurements$AllValid[[1]])>0 &&
#                                  length(service$Measurements$AnyValid[[1]])>0){
#           all(
#             all(mapply(FUN = function(x) nrow(measurementsBuilding %>% filter(eval(parse(text=x))))>0, 
#                        service$Measurements$AllValid[[1]])),
#             any(mapply(function(x) nrow(sBuilding %>% filter(eval(parse(text=x))))>0, 
#                        service$Measurements$AnyValid[[1]]))
#           )
#         } else if(length(service$Measurements$AllValid[[1]])>0){
#           all(mapply(FUN = function(x) nrow(measurementsBuilding %>% filter(eval(parse(text=x))))>0, 
#                      service$Measurements$AllValid[[1]]))
#         } else if(length(service$Measurements$AnyValid[[1]])>0){
#           any(mapply(FUN = function(x) nrow(measurementsBuilding %>% filter(eval(parse(text=x))))>0, 
#                      service$Measurements$AnyValid[[1]]))
#         } else {
#           NA
#         }
#         
#         # Building checking
#         Building_check <- if(length(service$Building$AllValid[[1]])>0 &&
#                              length(service$Building$AnyValid[[1]])>0){
#           all(
#             all(mapply(FUN = function(x) nrow(staticsBuilding %>% filter(eval(parse(text=x))))>0,
#                        #eval(parse(text=gsub("()","(buildingsRdf=buildingsRdf, buildingSubject=buildingSubject)",x,fixed = T))), 
#                        service$Building$AllValid[[1]])),
#             any(mapply(function(x) nrow(staticsBuilding %>% filter(eval(parse(text=x))))>0,
#                        #eval(parse(text=gsub("()","(buildingsRdf=buildingsRdf, buildingSubject=buildingSubject)",x,fixed = T))), 
#                        service$Building$AnyValid[[1]]))
#           )
#         } else if(length(service$Building$AllValid[[1]])>0){
#           all(mapply(FUN = function(x) nrow(staticsBuilding %>% filter(eval(parse(text=x))))>0,
#                      #eval(parse(text=gsub("()","(buildingsRdf=buildingsRdf, buildingSubject=buildingSubject)",x,fixed = T))), 
#                      service$Building$AllValid[[1]]))
#         } else if(length(service$Building$AnyValid[[1]])>0){
#           any(mapply(FUN = function(x) nrow(staticsBuilding %>% filter(eval(parse(text=x))))>0,
#                      #eval(parse(text=gsub("()","(buildingsRdf=buildingsRdf, buildingSubject=buildingSubject)",x,fixed = T))), 
#                      service$Building$AnyValid[[1]]))
#         } else {
#           NA
#         }
#         
#         c(
#           "CompliesAllRequirements" = all(DeviceAggregators_check, Measurements_check, Building_check,na.rm=T),
#           "CompliesDeviceAggregatorsRequirements" = DeviceAggregators_check,
#           "CompliesMeasurementsRequirements" = Measurements_check,
#           "CompliesBuildingRequirements" = Building_check
#         )
#       }, 1:nrow(settings$DataRequirementsForAnalyticalServices)))
#     )
#     if(nrow(measurementsBuilding)>0){
#       measurementsBuilding %>% select(-buildingSubject)
#     }
#     if(nrow(deviceAggregatorsBuilding)>0){
#       deviceAggregatorsBuilding %>% select(-buildingSubject)
#     }
#     list(
#          "BuildingSubject" = buildingSubject,
#          "Measurements" = measurementsBuilding,
#          "DeviceAggregators" = deviceAggregatorsBuilding,
#          "Statics" = staticsBuilding %>% select(-buildingSubject),
#          "Services" = services,
#          "_updated" = format_iso_8601z(lubridate::with_tz(Sys.time(),"UTC")))
#   })
#   
#   suppressMessages(suppressWarnings(library(mongolite)))
#   if(mongo_check("", settings)){
#     write("Loading the results to Mongo",stderr())
#     for (item in checkedResults){
#       mongo_conn("DataRequirementsCompliance", settings)$replace(
#         query=sprintf('{"BuildingSubject": "%s"}',item$BuildingSubject),
#         update=jsonlite::toJSON(c(list('BuildingSubject'=jsonlite::unbox(item[['BuildingSubject']])),
#                                   list('_updated'=jsonlite::unbox(item[['_updated']])),
#                                   item[!(names(item) %in% c('BuildingSubject','_updated'))]), na = 'null'),upsert=T)
#     }
#   }
#   write("Data requirements compliance successfully executed!",stderr())
#   return(checkedResults)
# }

#
# Read time series from devices ----
#

#' Get sensor metadata
#' 
#' This function gets the available metadata of a certain sensor time series.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param sensorId <uri> or directly the hash of a measurement.
#' @param tz <string> specifying the local time zone related to the
#' building in analysis. The format of this time zones are defined by the IANA
#' Time Zone Database (https://www.iana.org/time-zones).
#' @return <data.frame> with the metadata of the sensor identifier.

get_measurement_metadata_v2 <- function(buildingsRdf, measurementId, tz){
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?m ?hasMeasurement ?timeSeriesFrequency ?timeSeriesIsCumulative
    ?timeSeriesTimeAggregationFunction ?timeSeriesIsOnChange ?timeSeriesIsRegular
    ?measuredProperty ?considerEstimatedValues ?deviceName
    WHERE {
      {
        SELECT ?m ?hasMeasurement 
        WHERE {
          ?m a saref:Device.
          ?m saref:makesMeasurement ?hasMeasurement .
          ?hasMeasurement bigg:hash ?hash .
          FILTER (str(?hash)= "',measurementId,'")
          ?m saref:measuresProperty ?prop .
          ?prop a saref:Property
        }
      }
      optional {?prop bigg:Aggregation_Function ?timeSeriesTimeAggregationFunction .}
      optional {?hasMeasurement bigg:timeSeriesIsCumulative ?timeSeriesIsCumulative .}
      optional {?hasMeasurement bigg:timeSeriesIsOnChange ?timeSeriesIsOnChange .}
      optional {?hasMeasurement bigg:timeSeriesIsRegular ?timeSeriesIsRegular .}
      optional {?m saref:measuresProperty ?measuredProperty .}
      optional {?m foaf:name ?deviceName .}
      optional {?hasMeasurement bigg:measurementFrequency ?timeSeriesFrequency .}
      optional {?hasMeasurement bigg:hasEstimationMethod ?hasEstimationMethod .}
      optional {?hasEstimationMethod bigg:considerEstimatedValues ?considerEstimatedValues .}
    }')))
  metadata_df <- metadata_df %>%
    group_by(across(all_of(setdiff(colnames(metadata_df), "timeSeriesTimeAggregationFunction")))) %>%
    summarise(timeSeriesTimeAggregationFunction = list(timeSeriesTimeAggregationFunction), .groups = "drop")
  metadata_df$tz <- tz
  metadata_df$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                       metadata_df$measuredProperty)
  metadata_df$timeSeriesFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                   as.period(metadata_df$timeSeriesFrequency)),
                                            metadata_df$timeSeriesFrequency,NA)
  metadata_df$measurementId <- measurementId
  com <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?s ?deviceCommodity
    WHERE {
      ?s s4ener:hasCommodity ?deviceCommodity .
      ?s a s4syst:System.
    }')))
  for (system in com$s) {
    if (metadata_df$m %in% find_related_subjects_per_type(buildingsRdf, system, "s4syst:hasSubSystem", "s4syst:System")) {
      metadata_df$deviceCommodity <- com[(com$s==system),]$deviceCommodity
    }
  }
  
  return(metadata_df)
}


#' Get space properties measurement metadata
#' 
#' This function gets the available metadata of a certain sensor time series.
#'
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param sensorId <uri> or directly the hash of a measurement.
#' @param tz <string> specifying the local time zone related to the
#' building in analysis. The format of this time zones are defined by the IANA
#' Time Zone Database (https://www.iana.org/time-zones).
#' @return <data.frame> with the metadata of the sensor identifier.

get_space_properties_measurement_metadata_v2 <- function(buildingsRdf, sensorId, tz){
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?hasMeasurement ?timeSeriesFrequency ?timeSeriesIsCumulative
    ?timeSeriesTimeAggregationFunction ?timeSeriesIsOnChange ?timeSeriesIsRegular
    ?measuredProperty ?considerEstimatedValues
    WHERE {
      {
        SELECT ?m ?hasMeasurement ?prop
        WHERE {
          ?hasMeasurement a saref:Measurement.
          ?hasMeasurement saref:relatesToProperty ?prop .
          ?prop a saref:Property .
          ?hasMeasurement bigg:hash ?hash .
          FILTER (str(?hash)= "',sensorId,'") .
        }
      }
      optional {?prop bigg:Aggregation_Function ?timeSeriesTimeAggregationFunction .}
      optional {?hasMeasurement bigg:timeSeriesIsCumulative ?timeSeriesIsCumulative .}
      optional {?hasMeasurement bigg:timeSeriesIsOnChange ?timeSeriesIsOnChange .}
      optional {?hasMeasurement bigg:timeSeriesIsRegular ?timeSeriesIsRegular .}
      optional {?hasMeasurement saref:relatesToProperty ?measuredProperty .}
      optional {?hasMeasurement bigg:measurementFrequency ?timeSeriesFrequency .}
      optional {?hasMeasurement bigg:hasEstimationMethod ?hasEstimationMethod .}
      optional {?hasEstimationMethod bigg:considerEstimatedValues ?considerEstimatedValues .}
    }')))
  metadata_df$tz <- tz
  metadata_df$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                       metadata_df$measuredProperty)
  metadata_df$timeSeriesFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                   as.period(metadata_df$timeSeriesFrequency)),
                                            metadata_df$timeSeriesFrequency,NA)
  metadata_df$sensorId <- sensorId
  return(metadata_df)
}


#' Read time series files and generate a list of them split by sensor identifiers
#' 
#' This function gets time series from JSON files that has the name of a 
#' selected sensor identifier. If a list of time series is provided, this function
#' only filters the input list considering the sensor identifiers. 
#'
#' @param timeseriesObject <string> describing the local path with the JSON 
#' files or a <list> containing the time series.
#' @param measurementId <string> identifying a time series. 
#' @return <list> of data.frames with all time series found.

get_measurement_file_v2 <- function(timeseriesObject,measurementId){
  if(is.character(timeseriesObject)){
    jsonFiles <- list.files(timeseriesObject,"*.json",full.names=T)
    timeseriesObject_ <- unlist(lapply(
      jsonFiles[grepl(measurementId,jsonFiles)],
      function(x){jsonlite::fromJSON(x)}),recursive=F)
    if (length(timeseriesObject_)==0){
      tsvFiles <- list.files(timeseriesObject,"*.tsv",full.names=T)
      timeseriesObject_ <- setNames(lapply(
        tsvFiles[grepl(measurementId,tsvFiles)],
        function(x){read.csv(x,sep = "\t")}),
        gsub(".tsv","",basename(tsvFiles[grepl(measurementId,tsvFiles)])))
      timeseriesObject_[[1]]$value <- as.numeric(timeseriesObject_[[1]]$value)
      timeseriesObject_[[1]]$isReal <- as.logical(timeseriesObject_[[1]]$isReal)
      timeseriesObject_[[1]]$isReal <- ifelse(is.na(timeseriesObject_[[1]]$isReal),F,
                                              timeseriesObject_[[1]]$isReal)
    }
  } else {
    timeseriesObject_ <- timeseriesObject[names(timeseriesObject) %in% measurementId]
  }
  return(timeseriesObject_)
}

#' Read and prepare a time series
#' 
#' This function get a raw time series related with a sensor and transform it to the one with 
#' the required characteristics (e.g. time aggregation, time alignment, cumulative to instantaneous, 
#' irregular to regular time steps...). It also integrates the calculation of the energy cost and
#' energy emissions.
#' 
#' @param timeseriesObject <string> path of JSON files, or <list> of time series.
#' @param buildingSubject <uri> containing the building subject.
#' @param sensorId <string> containing the sensor identifier.
#' @param tz <string> specifying the local time zone related to the
#' building in analysis. The format of this time zones are defined by the IANA
#' Time Zone Database (https://www.iana.org/time-zones).
#' @param outputFrequency <string> defining the frequency selected as output. 
#' It must follow ISO 8601 format representing the time step.
#' @param aggFunctions <string> describing the possible aggregation functions of the
#' resultant time series. Possible values: 'SUM', 'AVG', 'HDD', 'CDD'.
#' @param useEstimatedValues <boolean> describing if the estimated values of time series 
#' should be taken into account.
#' @param ratioCorrection <boolean> describing whether a ratio correction should be done, 
#' or not. Important to set to TRUE when time series contain gaps.
#' @return <data.frame> containing the resultant time series.

get_sensor_v2 <- function(timeseriesObject, buildingsRdf, sensorId, tz=NULL, outputFrequency=NULL, aggFunctions=NULL,
                       useEstimatedValues=F, integrateCost=T, integrateEmissions=T, 
                       transformToAggregatableMeasuredProperty=F, aggregatableMeasuredPropertyName=NULL, 
                       defaultFactorsByMeasuredProperty=NULL, obtainMetadata=F ){
  
  # Get period and aggregation function specific for the timeseries
  if(is.null(tz)){
    tz <- get_tz_measurement_v2(buildingsRdf, sensorId)
  }
  metadata <- get_measurement_metadata_v2(buildingsRdf, sensorId, tz)
  if(is.null(aggFunctions)){
    possibleAggFunctions <- list(
      "Temperature"="AVG",
      "HumidityRatio"="AVG",
      "Power"="AVG",
      "Electricity"="SUM",
      "Gas"="SUM",
      "Energy"="SUM"
    )
    if(grepl(paste(names(possibleAggFunctions),collapse="|"),metadata$measuredProperty))
      aggFunctions <- possibleAggFunctions[mapply(function(x)grepl(x,metadata$measuredProperty),names(possibleAggFunctions))][[1]]
    else {
      aggFunctions <- "SUM"
    }
  } 
  if(is.null(outputFrequency)){
    outputFrequency <- if(!is.na(metadata$timeSeriesFrequency)){
                          metadata$timeSeriesFrequency}else{"P1M"}
  }
  if(integrateCost){
    metadata_tariff <- get_tariff_metadata_v2(buildingsRdf, sensorId)
  } else {
    metadata_tariff <- data.frame()
  }
  if(integrateEmissions){
    metadata_emissions <- get_emissions_metadata_v2(buildingsRdf, sensorId, deviceName=metadata$deviceName, deviceCommodity=metadata$deviceCommodity)
  } else {
    metadata_emissions <- data.frame()
  }
  
  # If timeseriesObject is NULL, read certain sensor
  timeseriesObject_ <- tryCatch({
    if(!is.list(timeseriesObject)){
      get_measurement_file_v2(timeseriesObject,metadata$measurementId)
    } else {
      timeseriesObject_ <- timeseriesObject[names(timeseriesObject) %in% sensorId]
    }
    }, error=function(x){
      NULL
    })
  if(is.null(timeseriesObject_) || length(timeseriesObject_)==0 || identical(timeseriesObject_[[1]],list())){
    if(obtainMetadata){
      metadata$timeSeriesStart <- NA
      metadata$timeSeriesEnd <- NA
      return(list(
              "timeseries"=NULL,
              "metadata"=metadata
            ))
    } else {
      stop(sprintf("Measurement Hash %s (%s) is not available in the time series of the building.",
                   sensorId, metadata$measuredProperty[1]))
    }
  }
  
  timeseriesSensor <- timeseriesObject_[sensorId][[1]]
  timeseriesSensor$start <- parse_iso_8601(timeseriesSensor$start)
  timeseriesSensor$end <- parse_iso_8601(timeseriesSensor$end)
  metadata$timeSeriesStart <- min(timeseriesSensor$start)
  metadata$timeSeriesEnd <- max(timeseriesSensor$end)
  timeseriesSensor <- timeseriesSensor[order(timeseriesSensor$start),]
  if(!("isReal" %in% colnames(timeseriesSensor))){
    timeseriesSensor$isReal <- T
  }
  
  # Non-regular timeseries
  if(metadata$timeSeriesIsRegular==F){
    # If value data is cumulative, integrate the series
    if(metadata$timeSeriesIsCumulative){
      timeseriesSensor$start <- lag(timeseriesSensor$start,1)
      timeseriesSensor$end <- timeseriesSensor$end - lubridate::seconds(1)
      timeseriesSensor$value <- timeseriesSensor$value - lag(timeseriesSensor$value,1)
      timeseriesSensor <- timeseriesSensor[is.finite(timeseriesSensor$start),]
    }
    
    timesteps <- data.frame(
      secs = c(1,60,3600,86400),
      freq = c("PT1S","PT1M","PT1H","P1D"))
    
    # If values are on change, reconsider the start and end timestamps
    if(metadata$timeSeriesIsOnChange){
      aux <- timeseriesSensor$end + lubridate::period(metadata$timeSeriesFrequency)
      aux2 <- aux < lead(timeseriesSensor$start,1)
      aux2[is.na(aux2)] <- T
      timeseriesSensor$end <- lubridate::as_datetime(
        ifelse(aux2, aux, lead(timeseriesSensor$start,1)))
      interpolateFrequency <- lubridate::format_ISO8601(lubridate::as.period(
        quantile(difftime(lead(timeseriesSensor$start,1),timeseriesSensor$start,
                          tz = "UTC",units = "secs"),
                 0.1,na.rm=T)))
    } else {
      # Calculate the minimum frequency for series interpolation
      aux <- difftime(timeseriesSensor$end, timeseriesSensor$start,
                            tz = "UTC",units = "secs")
      aux <- ifelse(aux>0,aux,NA)
      i_timestep <- which(timesteps$secs>=
              as.numeric(lubridate::as.period(
                quantile(aux, 0.1, na.rm=T)
              )))
      i_timestep <- if(length(i_timestep)==0){nrow(timesteps)} else {i_timestep[1]-1}
      interpolateFrequency <- timesteps[i_timestep,"freq"]
    }
    if(lubridate::as.period(outputFrequency) < lubridate::as.period(interpolateFrequency)){
      interpolateFrequency <- outputFrequency
    }
    metadata$timeSeriesFrequency <- interpolateFrequency
    
    # Detect the subsets with no internal gaps
    timeseriesSensor$gapAfter <- ifelse(
      difftime(timeseriesSensor$start,lag(timeseriesSensor$end,1),units = "secs") > as.numeric(as.period(interpolateFrequency)),
      1,0)
    timeseriesSensor$gapAfter <- cumsum(ifelse(is.na(timeseriesSensor$gapAfter),0,timeseriesSensor$gapAfter))
    # Resample the original irregular series to a regular series, among the detected subsets
    dfs <- lapply(split(timeseriesSensor,timeseriesSensor$gapAfter), function(tsChunk){
      # tsChunk <- split(timeseriesSensor,timeseriesSensor$gapAfter)[[2]]
      if("AVG" %in% aggFunctions){
        tsChunk$iniValue <- tsChunk$value
        tsChunk$iniIsReal <- tsChunk$isReal
      } else {
        tsChunk$value <- cumsum(tsChunk$value)
        tsChunk$iniValue <- lag(tsChunk$value,1)
        tsChunk$iniIsReal <- lag(tsChunk$isReal,1)
        tsChunk$iniValue[1] <- 0
        tsChunk <- tsChunk[cumsum(tsChunk$isReal)!=0 &
          rev(cumsum(replace_na(rev(tsChunk$isReal), 0))!=0),]
      }
      if(nrow(tsChunk)==0 || (nrow(tsChunk)==1 && is.na(tsChunk$iniValue))){
        return(NULL)
      }
      tsChunk <- rbind(
        data.frame("time"=tsChunk$start,"value"=tsChunk$iniValue,"isReal"=tsChunk$isReal),
        data.frame("time"=tsChunk$end,"value"=tsChunk$value,"isReal"=tsChunk$isReal))
      tsChunk$time <- lubridate::force_tz(lubridate::round_date(
        tsChunk$time,
        unit = iso8601_period_to_text_v2(interpolateFrequency,only_first = T),
        week_start = getOption("lubridate.week.start", 7)),tz)
      tsChunk <- tsChunk[!duplicated(tsChunk$time),]
      tsChunk <- tsChunk[order(tsChunk$time),]
      if(metadata$considerEstimatedValues==F || useEstimatedValues==F) tsChunk <- tsChunk[tsChunk$isReal==T,]
      tsChunk <- tsChunk %>% 
        padr::pad(interval = iso8601_period_to_text_v2(interpolateFrequency,only_first = T),
                  by = "time") %>%
        mutate(#time=as.POSIXct(lubridate::with_tz(time,"UTC"),tz="UTC"),
          value=zoo::na.approx(value,na.rm = F),
          isReal=zoo::na.locf(isReal))
      if(!("AVG" %in% aggFunctions)){
        tsChunk$value <- c(diff(tsChunk$value),NA)
      }
      return(tsChunk)
    })
    # Aggregation function used for the
    func <- function(x){ 
      if("AVG" %in% aggFunctions) {
        if(all(is.na(x))) NA else mean(x[!is.na(x)])
      } else { if(all(is.na(x))) NA else sum(x[!is.na(x)]) }
    }
    
    timeseriesSensor <- padr::pad(
      do.call(rbind,dfs) %>% 
        group_by(time) %>%
        summarise(value = func(value),
                  isReal = any(isReal,na.rm = T))
    )
    timeseriesSensor$isReal <- ifelse(is.finite(timeseriesSensor$isReal),timeseriesSensor$isReal,F)
    
    # Regular timeseries
  } else {
    timeseriesSensor$time <- timeseriesSensor$start
    timeseriesSensor$start <- NULL
    timeseriesSensor$end <- NULL
    timeseriesSensor <- padr::pad(timeseriesSensor,
                                  interval = iso8601_period_to_text_v2(
                                    metadata$timeSeriesFrequency,only_first = T))
  }
  # Align time grid
  timeseriesSensor$time <- lubridate::round_date(
    timeseriesSensor$time,
    unit = iso8601_period_to_text_v2(metadata$timeSeriesFrequency,only_first = T),
    week_start = getOption("lubridate.week.start", 7)
  )
  if(lubridate::period(outputFrequency)<lubridate::period("P1D")){
    aggFunctions <- aggFunctions[!(aggFunctions %in% c("HDD","CDD"))]
  }
  timeseriesSensor <- align_time_grid(
    data = timeseriesSensor,
    timeColumn = "time",
    valueColumn = "value",
    isRealColumn = "isReal",
    outputFrequency = outputFrequency,
    # if(any(c("HDD", "CDD") %in% aggFunctions)){
    #   "P1D" } else { outputFrequency },
    aggregationFunctions = aggFunctions,
    aggregationFunctionsSuffix = metadata$measuredProperty,
    # if(any(c("HDD", "CDD") %in% aggFunctions)){
    #   unique(c("AVG",aggFunctions[aggFunctions %in% c("SUM","MIN","MAX")]))
    # } else { 
    #   aggFunctions[aggFunctions %in% c("SUM","AVG","MIN","MAX")] 
    # },
    useEstimatedValues = metadata$considerEstimatedValues==T || useEstimatedValues==T,
    tz = metadata$tz
  )
  
  # Add energy cost component
  if(nrow(metadata_tariff)>0 & integrateCost){
    tryCatch(
      timeseriesSensor <- append_cost_to_sensor_v2(
        buildingsRdf, timeseriesObject, 
        tariffSubject = metadata_tariff$tariff,
        measuredProperty = metadata$measuredProperty,
        frequency = metadata$timeSeriesFrequency,
        energyTimeseriesSensor = timeseriesSensor),
      error = function(e) NULL
    )
  }
  # Add emissions component
  if(nrow(metadata_emissions)>0 & integrateEmissions){
    tryCatch(
      timeseriesSensor <- append_emissions_to_sensor_v2(
        buildingsRdf, timeseriesObject, 
        emissionsSubject = metadata_emissions$emissions,
        measuredProperty = metadata$measuredProperty,
        deviceName = metadata$deviceName,
        deviceCommodity = metadata$deviceCommodity,
        frequency = metadata$timeSeriesFrequency,
        energyTimeseriesSensor = timeseriesSensor),
    error = function(e) NULL
    )
  }
  
  # Transform the sensor to an aggregatable measured property
  if(transformToAggregatableMeasuredProperty & !is.null(timeseriesSensor)){
    timeseriesSensor <- sensor_measured_property_to_aggregatable_transformation_v2(
      buildingsRdf, timeseriesObject, 
      timeseriesSensor = timeseriesSensor,
      oldMeasuredProperty = metadata$measuredProperty,
      newMeasuredProperty = aggregatableMeasuredPropertyName,
      defaultFactorsByMeasuredProperty = defaultFactorsByMeasuredProperty
    )
  }
  if(obtainMetadata){
    return(list("timeseries"=timeseriesSensor,
                "metadata"=metadata))
  } else {
    return(timeseriesSensor)
  }
}


#' Read and prepare a time series
#' 
#' This function get a raw time series related with a sensor and transform it to the one with 
#' the required characteristics (e.g. time aggregation, time alignment, cumulative to instantaneous, 
#' irregular to regular time steps...). It also integrates the calculation of the energy cost and
#' energy emissions.
#' 
#' @param timeseriesObject <string> path of JSON files, or <list> of time series.
#' @param buildingSubject <uri> containing the building subject.
#' @param sensorId <string> containing the sensor identifier.
#' @param tz <string> specifying the local time zone related to the
#' building in analysis. The format of this time zones are defined by the IANA
#' Time Zone Database (https://www.iana.org/time-zones).
#' @param outputFrequency <string> defining the frequency selected as output. 
#' It must follow ISO 8601 format representing the time step.
#' @param aggFunctions <string> describing the possible aggregation functions of the
#' resultant time series. Possible values: 'SUM', 'AVG', 'HDD', 'CDD'.
#' @param useEstimatedValues <boolean> describing if the estimated values of time series 
#' should be taken into account.
#' @param ratioCorrection <boolean> describing whether a ratio correction should be done, 
#' or not. Important to set to TRUE when time series contain gaps.
#' @return <data.frame> containing the resultant time series.

get_space_measurement_v2 <- function(timeseriesObject, buildingsRdf, sensorId, tz=NULL, outputFrequency=NULL, aggFunctions=NULL,
                          useEstimatedValues=F, integrateCost=F, integrateEmissions=F, 
                          transformToAggregatableMeasuredProperty=F, aggregatableMeasuredPropertyName=NULL, 
                          defaultFactorsByMeasuredProperty=NULL, obtainMetadata=F ){
  
  # Get period and aggregation function specific for the timeseries
  if(is.null(tz)){
    tz <- get_tz_measurement_v2(buildingsRdf, sensorId)
  }
  metadata <- get_space_properties_measurement_metadata_v2(buildingsRdf, sensorId, tz)
  if(is.null(aggFunctions)){
    possibleAggFunctions <- list(
      "Temperature"="AVG",
      "HumidityRatio"="AVG",
      "Power"="AVG",
      "Electricity"="SUM",
      "Gas"="SUM",
      "Energy"="SUM"
    )
    if(grepl(paste(names(possibleAggFunctions),collapse="|"),metadata$measuredProperty))
      aggFunctions <- possibleAggFunctions[mapply(function(x)grepl(x,metadata$measuredProperty),names(possibleAggFunctions))][[1]]
    else {
      aggFunctions <- "SUM"
    }
  } 
  if(is.null(outputFrequency)){
    outputFrequency <- if(!is.na(metadata$timeSeriesFrequency)){
      metadata$timeSeriesFrequency}else{"P1M"}
  }
  if(integrateCost){
    metadata_tariff <- get_tariff_metadata_v2(buildingsRdf, sensorId)
  } else {
    metadata_tariff <- data.frame()
  }
  if(integrateEmissions){
    metadata_emissions <- get_emissions_metadata_v2(buildingsRdf, sensorId, deviceName=metadata$deviceName, deviceCommodity=metadata$deviceCommodity)
  } else {
    metadata_emissions <- data.frame()
  }
  
  # If timeseriesObject is NULL, read certain sensor
  timeseriesObject_ <- tryCatch({
    if(!is.list(timeseriesObject)){
      get_measurement_file_v2(timeseriesObject,metadata$measurementId)
    } else {
      timeseriesObject_ <- timeseriesObject[names(timeseriesObject) %in% sensorId]
    }
  }, error=function(x){
    NULL
  })
  if(is.null(timeseriesObject_) || length(timeseriesObject_)==0 || identical(timeseriesObject_[[1]],list())){
    if(obtainMetadata){
      metadata$timeSeriesStart <- NA
      metadata$timeSeriesEnd <- NA
      return(list(
        "timeseries"=NULL,
        "metadata"=metadata
      ))
    } else {
      stop(sprintf("Measurement Hash %s (%s) is not available in the time series of the building.",
                   sensorId, metadata$measuredProperty[1]))
    }
  }
  
  timeseriesSensor <- timeseriesObject_[sensorId][[1]]
  timeseriesSensor$start <- parse_iso_8601(timeseriesSensor$start)
  timeseriesSensor$end <- parse_iso_8601(timeseriesSensor$end)
  metadata$timeSeriesStart <- min(timeseriesSensor$start)
  metadata$timeSeriesEnd <- max(timeseriesSensor$end)
  timeseriesSensor <- timeseriesSensor[order(timeseriesSensor$start),]
  if(!("isReal" %in% colnames(timeseriesSensor))){
    timeseriesSensor$isReal <- T
  }
  
  # Non-regular timeseries
  if(metadata$timeSeriesIsRegular==F){
    # If value data is cumulative, integrate the series
    if(metadata$timeSeriesIsCumulative){
      timeseriesSensor$start <- lag(timeseriesSensor$start,1)
      timeseriesSensor$end <- timeseriesSensor$end - lubridate::seconds(1)
      timeseriesSensor$value <- timeseriesSensor$value - lag(timeseriesSensor$value,1)
      timeseriesSensor <- timeseriesSensor[is.finite(timeseriesSensor$start),]
    }
    
    timesteps <- data.frame(
      secs = c(1,60,3600,86400),
      freq = c("PT1S","PT1M","PT1H","P1D"))
    
    # If values are on change, reconsider the start and end timestamps
    if(metadata$timeSeriesIsOnChange){
      aux <- timeseriesSensor$end + lubridate::period(metadata$timeSeriesFrequency)
      aux2 <- aux < lead(timeseriesSensor$start,1)
      aux2[is.na(aux2)] <- T
      timeseriesSensor$end <- lubridate::as_datetime(
        ifelse(aux2, aux, lead(timeseriesSensor$start,1)))
      interpolateFrequency <- lubridate::format_ISO8601(lubridate::as.period(
        quantile(difftime(lead(timeseriesSensor$start,1),timeseriesSensor$start,
                          tz = "UTC",units = "secs"),
                 0.1,na.rm=T)))
    } else {
      # Calculate the minimum frequency for series interpolation
      aux <- difftime(timeseriesSensor$end, timeseriesSensor$start,
                      tz = "UTC",units = "secs")
      aux <- ifelse(aux>0,aux,NA)
      i_timestep <- which(timesteps$secs>=
                            as.numeric(lubridate::as.period(
                              quantile(aux, 0.1, na.rm=T)
                            )))
      i_timestep <- if(length(i_timestep)==0){nrow(timesteps)} else {i_timestep[1]-1}
      interpolateFrequency <- timesteps[i_timestep,"freq"]
    }
    if(lubridate::as.period(outputFrequency) < lubridate::as.period(interpolateFrequency)){
      interpolateFrequency <- outputFrequency
    }
    metadata$timeSeriesFrequency <- interpolateFrequency
    
    # Detect the subsets with no internal gaps
    timeseriesSensor$gapAfter <- ifelse(
      difftime(timeseriesSensor$start,lag(timeseriesSensor$end,1),units = "secs") > as.numeric(as.period(interpolateFrequency)),
      1,0)
    timeseriesSensor$gapAfter <- cumsum(ifelse(is.na(timeseriesSensor$gapAfter),0,timeseriesSensor$gapAfter))
    # Resample the original irregular series to a regular series, among the detected subsets
    dfs <- lapply(split(timeseriesSensor,timeseriesSensor$gapAfter), function(tsChunk){
      # tsChunk <- split(timeseriesSensor,timeseriesSensor$gapAfter)[[2]]
      if("AVG" %in% aggFunctions){
        tsChunk$iniValue <- tsChunk$value
        tsChunk$iniIsReal <- tsChunk$isReal
      } else {
        tsChunk$value <- cumsum(tsChunk$value)
        tsChunk$iniValue <- lag(tsChunk$value,1)
        tsChunk$iniIsReal <- lag(tsChunk$isReal,1)
        tsChunk$iniValue[1] <- 0
        tsChunk <- tsChunk[cumsum(tsChunk$isReal)!=0 &
                             rev(cumsum(rev(tsChunk$isReal))!=0),]
      }
      if(nrow(tsChunk)==0 || (nrow(tsChunk)==1 && is.na(tsChunk$iniValue))){
        return(NULL)
      }
      tsChunk <- rbind(
        data.frame("time"=tsChunk$start,"value"=tsChunk$iniValue,"isReal"=tsChunk$isReal),
        data.frame("time"=tsChunk$end,"value"=tsChunk$value,"isReal"=tsChunk$isReal))
      tsChunk$time <- lubridate::force_tz(lubridate::round_date(
        tsChunk$time,
        unit = iso8601_period_to_text_v2(interpolateFrequency,only_first = T),
        week_start = getOption("lubridate.week.start", 7)),tz)
      tsChunk <- tsChunk[!duplicated(tsChunk$time),]
      tsChunk <- tsChunk[order(tsChunk$time),]
      if(metadata$considerEstimatedValues==F || useEstimatedValues==F) tsChunk <- tsChunk[tsChunk$isReal==T,]
      tsChunk <- tsChunk %>% 
        padr::pad(interval = iso8601_period_to_text_v2(interpolateFrequency,only_first = T),
                  by = "time") %>%
        mutate(#time=as.POSIXct(lubridate::with_tz(time,"UTC"),tz="UTC"),
          value=zoo::na.approx(value,na.rm = F),
          isReal=zoo::na.locf(isReal))
      if(!("AVG" %in% aggFunctions)){
        tsChunk$value <- c(diff(tsChunk$value),NA)
      }
      return(tsChunk)
    })
    # Aggregation function used for the
    func <- function(x){ 
      if("AVG" %in% aggFunctions) {
        if(all(is.na(x))) NA else mean(x[!is.na(x)])
      } else { if(all(is.na(x))) NA else sum(x[!is.na(x)]) }
    }
    
    timeseriesSensor <- padr::pad(
      do.call(rbind,dfs) %>% 
        group_by(time) %>%
        summarise(value = func(value),
                  isReal = any(isReal,na.rm = T))
    )
    timeseriesSensor$isReal <- ifelse(is.finite(timeseriesSensor$isReal),timeseriesSensor$isReal,F)
    
    # Regular timeseries
  } else {
    timeseriesSensor$time <- timeseriesSensor$start
    timeseriesSensor$start <- NULL
    timeseriesSensor$end <- NULL
    timeseriesSensor <- padr::pad(timeseriesSensor,
                                  interval = iso8601_period_to_text_v2(
                                    metadata$timeSeriesFrequency,only_first = T))
  }
  # Align time grid
  timeseriesSensor$time <- lubridate::round_date(
    timeseriesSensor$time,
    unit = iso8601_period_to_text_v2(metadata$timeSeriesFrequency,only_first = T),
    week_start = getOption("lubridate.week.start", 7)
  )
  if(lubridate::period(outputFrequency)<lubridate::period("P1D")){
    aggFunctions <- aggFunctions[!(aggFunctions %in% c("HDD","CDD"))]
  }
  timeseriesSensor <- align_time_grid(
    data = timeseriesSensor,
    timeColumn = "time",
    valueColumn = "value",
    isRealColumn = "isReal",
    outputFrequency = outputFrequency,
    # if(any(c("HDD", "CDD") %in% aggFunctions)){
    #   "P1D" } else { outputFrequency },
    aggregationFunctions = aggFunctions,
    aggregationFunctionsSuffix = metadata$measuredProperty,
    # if(any(c("HDD", "CDD") %in% aggFunctions)){
    #   unique(c("AVG",aggFunctions[aggFunctions %in% c("SUM","MIN","MAX")]))
    # } else { 
    #   aggFunctions[aggFunctions %in% c("SUM","AVG","MIN","MAX")] 
    # },
    useEstimatedValues = metadata$considerEstimatedValues==T || useEstimatedValues==T,
    tz = metadata$tz
  )
  
  # Add energy cost component
  if(nrow(metadata_tariff)>0 & integrateCost){
    tryCatch(
      timeseriesSensor <- append_cost_to_sensor_v2(
        buildingsRdf, timeseriesObject, 
        tariffSubject = metadata_tariff$tariff,
        measuredProperty = metadata$measuredProperty,
        frequency = metadata$timeSeriesFrequency,
        energyTimeseriesSensor = timeseriesSensor),
      error = function(e) NULL
    )
  }
  # Add emissions component
  if(nrow(metadata_emissions)>0 & integrateEmissions){
    tryCatch(
      timeseriesSensor <- append_emissions_to_sensor_v2(
        buildingsRdf, timeseriesObject, 
        emissionsSubject = metadata_emissions$emissions,
        measuredProperty = metadata$measuredProperty,
        deviceName = metadata$deviceName,
        deviceCommodity = metadata$deviceCommodity,
        frequency = metadata$timeSeriesFrequency,
        energyTimeseriesSensor = timeseriesSensor),
      error = function(e) NULL
    )
  }
  
  # Transform the sensor to an aggregatable measured property
  if(transformToAggregatableMeasuredProperty & !is.null(timeseriesSensor)){
    timeseriesSensor <- sensor_measured_property_to_aggregatable_transformation_v2(
      buildingsRdf, timeseriesObject, 
      timeseriesSensor = timeseriesSensor,
      oldMeasuredProperty = metadata$measuredProperty,
      newMeasuredProperty = aggregatableMeasuredPropertyName,
      defaultFactorsByMeasuredProperty = defaultFactorsByMeasuredProperty
    )
  }
  if(obtainMetadata){
    return(list("timeseries"=timeseriesSensor,
                "metadata"=metadata))
  } else {
    return(timeseriesSensor)
  }
}


sensor_measured_property_to_aggregatable_transformation_v2 <- function(buildingsRdf, timeseriesObject, timeseriesSensor, 
                                                                    oldMeasuredProperty, newMeasuredProperty, 
                                                                    defaultFactorsByMeasuredProperty = NULL){
  # Get the transformation factors from the dataset
  # TO DO when defined in the ontology
  # if () {
  # } else
  if(!is.null(defaultFactorsByMeasuredProperty)){
    timeseriesFactor <- data.frame("time" = timeseriesSensor$time, 
                                   "factor"= if(oldMeasuredProperty %in% names(defaultFactorsByMeasuredProperty)){
                                     defaultFactorsByMeasuredProperty[[oldMeasuredProperty]]
                                   } else if ("Other" %in% names(defaultFactorsByMeasuredProperty)) {
                                     defaultFactorsByMeasuredProperty$Other
                                   } else {1})
  } else {
    timeseriesFactor <- data.frame("time" = timeseriesSensor$time, "factor"=1)
  }
  
  timeseriesSensor[grepl(paste0(oldMeasuredProperty,"$"),colnames(timeseriesSensor))] <- 
    timeseriesSensor[grepl(paste0(oldMeasuredProperty,"$"),colnames(timeseriesSensor))] * 
    timeseriesFactor$factor
  timeseriesSensor[grepl(paste0(oldMeasuredProperty,"_EnergyPrice$"),colnames(timeseriesSensor))] <- 
    timeseriesSensor[grepl(paste0(oldMeasuredProperty,"_EnergyPrice$"),colnames(timeseriesSensor))] * 
    timeseriesFactor$factor
  timeseriesSensor[grepl(paste0(oldMeasuredProperty,"_EnergyEmissionsFactor$"),colnames(timeseriesSensor))] <- 
    timeseriesSensor[grepl(paste0(oldMeasuredProperty,"_EnergyEmissionsFactor$"),colnames(timeseriesSensor))] * 
    timeseriesFactor$factor
  colnames(timeseriesSensor) <- gsub(oldMeasuredProperty, newMeasuredProperty, colnames(timeseriesSensor))
  
  return(timeseriesSensor)
}



get_consumption_device_aggregators_metadata_v2 <- function(buildingsRdf){
  
  consumptionsystems <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?cs
  WHERE {
     ?cs a bigg:ConsumptionSystem .
  }')))$cs
  
  cs <- c(consumptionsystems, find_related_subjects_per_type(buildingsRdf,consumptionsystems,"s4syst:hasSubSystem", "s4syst:System"))
  result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?system ?deviceAggregatorName ?hasMeasurement ?deviceAggregatorFrequency
  ?deviceAggregatorTimeAggregationFunction ?deviceAggregatorFormula ?measuredProperty
  WHERE {
    
    ?system a s4syst:System .
    FILTER ( ?system IN (<',paste(cs,collapse='>,<'),'>) ) .
    ?system s4syst:hasSubSystem ?device .
    ?device a saref:Device.
    ?device saref:makesMeasurement ?hasMeasurement .
    ?device saref:measuresProperty ?measuredProperty .
    
    optional {?device foaf:name ?deviceAggregatorName .}
    optional {?measuredProperty bigg:Aggregation_Function ?deviceAggregatorTimeAggregationFunction .}
    optional {?hasMeasurement bigg:measurementFrequency ?deviceAggregatorFrequency .}
    optional {?hasMeasurement bigg:measurementFormula ?deviceAggregatorFormula .}
  }')))
  
  if(length(result)>0){
    buildings <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?b
    WHERE {
       ?b a s4bldg:Building
    }')))$b
    for (building in buildings) {
      building_systems <- get_building_systems_list_v2(buildingsRdf, building)[[building]]
      for (i in 1:nrow(result)) {
        if (result[i,"system"] %in% building_systems) {
          result[i,"buildingSubject"] <- building
        }
      }
    }
    all_bspace_systems <- get_all_buildingspaces_systems_v2(buildingsRdf)
    result <- result %>% left_join(all_bspace_systems, by="system")
    result$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                    result$measuredProperty)
    result$deviceAggregatorFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                      as.period(result$deviceAggregatorFrequency)),
                                               result$deviceAggregatorFrequency,NA)
    com <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?s ?deviceCommodity
    WHERE {
      ?s s4ener:hasCommodity ?deviceCommodity .
      ?s a s4syst:System.
    }')))
    result <- result %>%
      mutate(deviceCommodity = NA)
    for (i in 1:nrow(result)) {
      for (system in com$s) {
        if (result[i,]$system %in% find_related_subjects_per_type(buildingsRdf, system, "s4syst:hasSubSystem", "s4syst:System")) {
          result[i,]$deviceCommodity <- com[(com$s==system),]$deviceCommodity
        }
      }
    }
    result$deviceCommodity <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                    result$deviceCommodity)
  }
  return(result)
}



#
# Read systems and buildingspaces aggregators ----
#


get_generation_device_aggregators_metadata_v2 <- function(buildingsRdf){
  
  generationsystems <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?gs
  WHERE {
     ?gs a bigg:GenerationSystem .
  }')))$gs
  
  gs <- c(generationsystems, find_related_subjects_per_type(buildingsRdf,generationsystems,"s4syst:hasSubSystem", "s4syst:System"))
  result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?system ?deviceAggregatorName ?hasMeasurement ?deviceAggregatorFrequency
  ?deviceAggregatorTimeAggregationFunction ?deviceAggregatorFormula ?measuredProperty
  WHERE {
    {
        ?system a s4syst:System .
        FILTER ( ?system IN (<',paste(gs,collapse='>,<'),'>) ) .
        ?system s4syst:hasSubSystem ?device .
        ?device a saref:Device.
        ?device saref:makesMeasurement ?hasMeasurement .
        ?device saref:measuresProperty ?measuredProperty .
    }
    optional {?measuredProperty bigg:Aggregation_Function ?deviceAggregatorTimeAggregationFunction .}
    optional {?device foaf:name ?deviceAggregatorName .}
    optional {?hasMeasurement bigg:measurementFrequency ?deviceAggregatorFrequency .}
    optional {?hasMeasurement bigg:measurementFormula ?deviceAggregatorFormula .}
  }')))
  
  result <- result %>%
    group_by(across(all_of(setdiff(colnames(result), "deviceAggregatorTimeAggregationFunction")))) %>%
    summarise(deviceAggregatorTimeAggregationFunction = list(deviceAggregatorTimeAggregationFunction), .groups = "drop")
  
  
  if(length(result)>0){
    buildings <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?b
    WHERE {
       ?b a s4bldg:Building
    }')))$b
    for (building in buildings) {
      building_systems <- get_building_systems_list_v2(buildingsRdf, building)[[building]]
      for (i in 1:nrow(result)) {
        if (result[i,"system"] %in% building_systems) {
          result[i,"buildingSubject"] <- building
        }
      }
    }
    all_bspace_systems <- get_all_buildingspaces_systems_v2(buildingsRdf)
    result <- result %>% left_join(all_bspace_systems, by="system")
    result$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                    result$measuredProperty)
    result$deviceAggregatorFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                      as.period(result$deviceAggregatorFrequency)),
                                               result$deviceAggregatorFrequency,NA)
    com <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?s ?deviceCommodity
    WHERE {
      ?s s4ener:hasCommodity ?deviceCommodity .
      ?s a s4syst:System.
    }')))
    result <- result %>%
      mutate(deviceCommodity = NA)
    for (i in 1:nrow(result)) {
      for (system in com$s) {
        if (result[i,]$system %in% find_related_subjects_per_type(buildingsRdf, system, "s4syst:hasSubSystem", "s4syst:System")) {
          result[i,]$deviceCommodity <- com[(com$s==system),]$deviceCommodity
        }
      }
    }
    result$deviceCommodity <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                   result$deviceCommodity)
  }
  return(result)
}


get_grid_device_aggregators_metadata_v2 <- function(buildingsRdf){
  
  gridsystems <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?gs
  WHERE {
     ?gs a bigg:GridSystem .
  }')))$gs
  
  gs <- c(gridsystems, find_related_subjects_per_type(buildingsRdf,gridsystems,"s4syst:hasSubSystem", "s4syst:System"))
  result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?system ?deviceAggregatorName ?hasMeasurement ?deviceAggregatorFrequency
  ?deviceAggregatorTimeAggregationFunction ?deviceAggregatorFormula ?measuredProperty
  WHERE {
    {
        ?system a s4syst:System .
        FILTER ( ?system IN (<',paste(gs,collapse='>,<'),'>) ) .
        ?system s4syst:hasSubSystem ?device .
        ?device a saref:Device.
        ?device saref:makesMeasurement ?hasMeasurement .
        ?device saref:measuresProperty ?measuredProperty .
    }
    optional {?measuredProperty bigg:Aggregation_Function ?deviceAggregatorTimeAggregationFunction .}
    optional {?device foaf:name ?deviceAggregatorName .}
    optional {?hasMeasurement bigg:measurementFrequency ?deviceAggregatorFrequency .}
    optional {?hasMeasurement bigg:measurementFormula ?deviceAggregatorFormula .}
  }')))
  
  result <- result %>%
    group_by(across(all_of(setdiff(colnames(result), "deviceAggregatorTimeAggregationFunction")))) %>%
    summarise(deviceAggregatorTimeAggregationFunction = list(deviceAggregatorTimeAggregationFunction), .groups = "drop")
  
  
  if(length(result)>0){
    buildings <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?b
    WHERE {
       ?b a s4bldg:Building
    }')))$b
    for (building in buildings) {
      building_systems <- get_building_systems_list_v2(buildingsRdf, building)[[building]]
      for (i in 1:nrow(result)) {
        if (result[i,"system"] %in% building_systems) {
          result[i,"buildingSubject"] <- building
        }
      }
    }
    all_bspace_systems <- get_all_buildingspaces_systems_v2(buildingsRdf)
    result <- result %>% left_join(all_bspace_systems, by="system")
    result$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                    result$measuredProperty)
    result$deviceAggregatorFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                      as.period(result$deviceAggregatorFrequency)),
                                               result$deviceAggregatorFrequency,NA)
    com <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?s ?deviceCommodity
    WHERE {
      ?s s4ener:hasCommodity ?deviceCommodity .
      ?s a s4syst:System.
    }')))
    result <- result %>%
      mutate(deviceCommodity = NA)
    for (i in 1:nrow(result)) {
      for (system in com$s) {
        if (result[i,]$system %in% find_related_subjects_per_type(buildingsRdf, system, "s4syst:hasSubSystem", "s4syst:System")) {
          result[i,]$deviceCommodity <- com[(com$s==system),]$deviceCommodity
        }
      }
    }
    result$deviceCommodity <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                   result$deviceCommodity)
  }
  return(result)
}

get_storage_device_aggregators_metadata_v2 <- function(buildingsRdf){
  
  storageystems <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?ss
  WHERE {
     ?ss a bigg:StorageSystem .
  }')))$ss
  
  ss <- c(storageystems, find_related_subjects_per_type(buildingsRdf,storageystems,"s4syst:hasSubSystem", "s4syst:System"))
  result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?system ?deviceAggregatorName ?hasMeasurement ?deviceAggregatorFrequency
  ?deviceAggregatorTimeAggregationFunction ?deviceAggregatorFormula ?measuredProperty
  WHERE {
    {
        ?system a s4syst:System .
        FILTER ( ?system IN (<',paste(ss,collapse='>,<'),'>) ) .
        ?system s4syst:hasSubSystem ?device .
        ?device a saref:Device.
        ?device saref:makesMeasurement ?hasMeasurement .
        ?device saref:measuresProperty ?measuredProperty .
    }
    optional {?measuredProperty bigg:Aggregation_Function ?deviceAggregatorTimeAggregationFunction .}
    optional {?device foaf:name ?deviceAggregatorName .}
    optional {?hasMeasurement bigg:measurementFrequency ?deviceAggregatorFrequency .}
    optional {?hasMeasurement bigg:measurementFormula ?deviceAggregatorFormula .}
  }')))
  
  result <- result %>%
    group_by(across(all_of(setdiff(colnames(result), "deviceAggregatorTimeAggregationFunction")))) %>%
    summarise(deviceAggregatorTimeAggregationFunction = list(deviceAggregatorTimeAggregationFunction), .groups = "drop")
  
  
  if(length(result)>0){
    buildings <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?b
    WHERE {
       ?b a s4bldg:Building
    }')))$b
    for (building in buildings) {
      building_systems <- get_building_systems_list_v2(buildingsRdf, building)[[building]]
      for (i in 1:nrow(result)) {
        if (result[i,"system"] %in% building_systems) {
          result[i,"buildingSubject"] <- building
        }
      }
    }
    all_bspace_systems <- get_all_buildingspaces_systems_v2(buildingsRdf)
    result <- result %>% left_join(all_bspace_systems, by="system")
    result$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                    result$measuredProperty)
    result$deviceAggregatorFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                      as.period(result$deviceAggregatorFrequency)),
                                               result$deviceAggregatorFrequency,NA)
    com <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?s ?deviceCommodity
    WHERE {
      ?s s4ener:hasCommodity ?deviceCommodity .
      ?s a s4syst:System.
    }')))
    result <- result %>%
      mutate(deviceCommodity = NA)
    for (i in 1:nrow(result)) {
      for (system in com$s) {
        if (result[i,]$system %in% find_related_subjects_per_type(buildingsRdf, system, "s4syst:hasSubSystem", "s4syst:System")) {
          result[i,]$deviceCommodity <- com[(com$s==system),]$deviceCommodity
        }
      }
    }
    result$deviceCommodity <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                   result$deviceCommodity)
  }
  return(result)
}

get_weather_device_aggregators_metadata_v2 <- function(buildingsRdf){
  
  weatherstationsystems <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?ws
  WHERE {
     ?ws a s4agri:WeatherStation .
  }')))$ws
  
  ws <- c(weatherstationsystems, find_related_subjects_per_type(buildingsRdf,weatherstationsystems,"s4syst:hasSubSystem", "s4syst:System"))
  result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?system ?deviceAggregatorName ?hasMeasurement ?deviceAggregatorFrequency
  ?deviceAggregatorTimeAggregationFunction ?deviceAggregatorFormula ?measuredProperty
  WHERE {
    {
        ?system a s4syst:System .
        FILTER ( ?system IN (<',paste(ws,collapse='>,<'),'>) ) .
        ?system s4syst:hasSubSystem ?device .
        ?device a saref:Device.
        ?device saref:makesMeasurement ?hasMeasurement .
        ?device saref:measuresProperty ?measuredProperty .
    }
    optional {?measuredProperty bigg:Aggregation_Function ?deviceAggregatorTimeAggregationFunction .}
    optional {?device foaf:name ?deviceAggregatorName .}
    optional {?hasMeasurement bigg:measurementFrequency ?deviceAggregatorFrequency .}
    optional {?hasMeasurement bigg:measurementFormula ?deviceAggregatorFormula .}
  }')))
  
  result <- result %>%
    group_by(across(all_of(setdiff(colnames(result), "deviceAggregatorTimeAggregationFunction")))) %>%
    summarise(deviceAggregatorTimeAggregationFunction = list(deviceAggregatorTimeAggregationFunction), .groups = "drop")
  
  
  if(length(result)>0){
    buildings <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?b
    WHERE {
       ?b a s4bldg:Building
    }')))$b
    for (building in buildings) {
      building_systems <- get_building_systems_list_v2(buildingsRdf, building)[[building]]
      for (i in 1:nrow(result)) {
        if (result[i,"system"] %in% building_systems) {
          result[i,"buildingSubject"] <- building
        }
      }
    }
    all_bspace_systems <- get_all_buildingspaces_systems_v2(buildingsRdf)
    result <- result %>% left_join(all_bspace_systems, by="system")
    result$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                    result$measuredProperty)
    result$deviceAggregatorFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                      as.period(result$deviceAggregatorFrequency)),
                                               result$deviceAggregatorFrequency,NA)
    
    result$deviceCommodity <- "NotCommodity"
  }
  return(result)
}

get_selected_device_aggregators_metadata_v2 <- function(buildingsRdf, selectedSystems){
  
  selected <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?ss
  WHERE {
     ?ss a ?type .
     FILTER(?type IN (', paste(selectedSystems, collapse = ", "), '))
  }')))$ss
  
  ss <- c(selected, find_related_subjects_per_type(buildingsRdf,selected,"s4syst:hasSubSystem", "s4syst:System"))
  result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?system ?deviceAggregatorName ?hasMeasurement ?deviceAggregatorFrequency
  ?deviceAggregatorTimeAggregationFunction ?deviceAggregatorFormula ?measuredProperty
  WHERE {
    {
        ?system a s4syst:System .
        FILTER ( ?system IN (<',paste(ss,collapse='>,<'),'>) ) .
        ?system s4syst:hasSubSystem ?device .
        ?device a saref:Device.
        ?device saref:makesMeasurement ?hasMeasurement .
        ?device saref:measuresProperty ?measuredProperty .
    }
    optional {?measuredProperty bigg:Aggregation_Function ?deviceAggregatorTimeAggregationFunction .}
    optional {?device foaf:name ?deviceAggregatorName .}
    optional {?hasMeasurement bigg:measurementFrequency ?deviceAggregatorFrequency .}
    optional {?hasMeasurement bigg:measurementFormula ?deviceAggregatorFormula .}
  }')))
  
  result <- result %>%
    group_by(across(all_of(setdiff(colnames(result), "deviceAggregatorTimeAggregationFunction")))) %>%
    summarise(deviceAggregatorTimeAggregationFunction = list(deviceAggregatorTimeAggregationFunction), .groups = "drop")
  
  if(length(result)>0){
    buildings <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?b
    WHERE {
       ?b a s4bldg:Building
    }')))$b
    for (building in buildings) {
      building_systems <- get_building_systems_list_v2(buildingsRdf, building)[[building]]
      for (i in 1:nrow(result)) {
        if (result[i,"system"] %in% building_systems) {
          result[i,"buildingSubject"] <- building
        }
      }
    }
    all_bspace_systems <- get_all_buildingspaces_systems_v2(buildingsRdf)
    result <- result %>% left_join(all_bspace_systems, by="system")
    result$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                    result$measuredProperty)
    result$deviceAggregatorFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                      as.period(result$deviceAggregatorFrequency)),
                                               result$deviceAggregatorFrequency,NA)
    com <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
    SELECT ?s ?deviceCommodity
    WHERE {
      ?s s4ener:hasCommodity ?deviceCommodity .
      ?s a s4syst:System.
    }')))
    result <- result %>%
      mutate(deviceCommodity = NA)
    for (i in 1:nrow(result)) {
      for (system in com$s) {
        if (result[i,]$system %in% find_related_subjects_per_type(buildingsRdf, system, "s4syst:hasSubSystem", "s4syst:System")) {
          result[i,]$deviceCommodity <- com[(com$s==system),]$deviceCommodity
        }
      }
    }
    result <- result %>% mutate(deviceCommodity = ifelse(grepl("weather", system), "NotCommodity", gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                                                                                             deviceCommodity)))
    # result$deviceCommodity <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
    #                                result$deviceCommodity)
  }
  return(result)
}


get_deployments_metadata_v2 <- function(buildingsRdf){
  
  # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?bz ?dp
    WHERE {
      ?bz a s4bldg:BuildingSpace .
      ?dp a s4agri:Deployment .
      ?dp s4agri:isDeployedAtSpace ?bz .
      }')))
  
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}

get_occupancy_metadata_v2 <- function(buildingsRdf){
  
  # candidates <- find_related_subjects_per_type(buildingsRdf, "https://gencat.cat#system-00001-CPD-districtClima", "s4syst:hasSubSystem", "bigg:BuildingSystem")
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?buildingSpace ?oc ?deviceAggregatorName ?hasMeasurement ?deviceAggregatorFrequency
    ?deviceAggregatorTimeAggregationFunction ?deviceAggregatorFormula ?measuredProperty
    WHERE {
      ?buildingSpace a s4bldg:BuildingSpace .
      ?buildingSpace bigg:hasOccupancy ?oc .
      ?oc a bigg:Occupancy .
      OPTIONAL {?oc foaf:name ?deviceAggregatorName .}
      ?oc saref:hasMeasurement ?hasMeasurement .
      OPTIONAL {?hasMeasurement bigg:measurementFrequency ?deviceAggregatorFrequency .}
      OPTIONAL {?hasMeasurement bigg:measurementFormula ?deviceAggregatorFormula .}
      ?hasMeasurement saref:relatesToProperty ?measuredProperty .
      OPTIONAL {?measuredProperty bigg:Aggregation_Function ?deviceAggregatorTimeAggregationFunction .}
    }')))
  
  if (length(metadata_df)>0) {
    buildings <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?b
      WHERE {
         ?b a s4bldg:Building
      }')))$b
    
    for (building in buildings) {
      building_zones <-get_all_building_subspaces_v2(buildingsRdf, building)[[building]]
      for (i in 1:nrow(metadata_df)) {
        if (metadata_df[i,"buildingSpace"] %in% building_zones) {
          metadata_df[i,"buildingSubject"] <- building
        }
      }
    }
    metadata_df$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                         metadata_df$measuredProperty)
    metadata_df$deviceAggregatorFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                      as.period(metadata_df$deviceAggregatorFrequency)),
                                                    metadata_df$deviceAggregatorFrequency,NA)
  }
  
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}

get_space_state_metadata_v2 <- function(buildingsRdf){
  
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?buildingSpace ?st ?deviceAggregatorName ?hasMeasurement ?deviceAggregatorFrequency
    ?deviceAggregatorTimeAggregationFunction ?deviceAggregatorFormula ?measuredProperty
    WHERE {
      ?buildingSpace a s4bldg:BuildingSpace .
      ?buildingSpace bee:hasSpaceState ?st .
      OPTIONAL {?st foaf:name ?deviceAggregatorName .}
      ?st saref:hasMeasurement ?hasMeasurement .
      OPTIONAL {?hasMeasurement bigg:measurementFrequency ?deviceAggregatorFrequency .}
      OPTIONAL {?hasMeasurement bigg:measurementFormula ?deviceAggregatorFormula .}
      ?hasMeasurement saref:relatesToProperty ?measuredProperty .
      OPTIONAL {?measuredProperty bigg:Aggregation_Function ?deviceAggregatorTimeAggregationFunction .}
    }')))
  
  if (length(metadata_df)>0) {
    buildings <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?b
      WHERE {
         ?b a s4bldg:Building
      }')))$b
  
    for (building in buildings) {
      building_zones <-get_all_building_subspaces_v2(buildingsRdf, building)[[building]]
      for (i in 1:nrow(metadata_df)) {
        if (metadata_df[i,"buildingSpace"] %in% building_zones) {
          metadata_df[i,"buildingSubject"] <- building
        }
      }
    }
    metadata_df$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                         metadata_df$measuredProperty)
    metadata_df$deviceAggregatorFrequency <- ifelse(mapply(function(x)!is.na(x),
                                                           as.period(metadata_df$deviceAggregatorFrequency)),
                                                    metadata_df$deviceAggregatorFrequency,NA)
  }
  return( 
    if(length(metadata_df)>0) {
      metadata_df
    } else { NULL } 
  )
}


#' Compute the specified formula of a device aggregator
#' 
#' This function obtains all the time series related with a device aggregator, aggregates them 
#' according to the device aggregator metadata and obtains a single resultant time series.
#' 
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param timeseriesObject <string> path of JSON files, or <list> of time series.
#' @param buildingSubject <uri> containing the building subject.
#' @param formula <string> describing the formula for the device aggregator. 
#' It consist on a sequence of arithmetical operations over one (or multiple) sensor 
#' identifier(s). The sensor identifiers must be written between prefix '<mi>' and suffix </mi>. 
#' On contrary, the operators are described between prefix <mo> and suffix </mo>.
#' Example of the sum of two sensors (let's identify them as 'ID1' and 'ID2'): 
#' '<mi>ID1</mi><mo>+</mo><mi>ID2</mi>'
#' @param outputFrequency <string> defining the frequency selected as output. 
#' It must follow ISO 8601 format representing the time step.
#' @param aggFunctions <string> describing the possible aggregation functions of the
#' resultant time series. Possible values: 'SUM', 'AVG', 'HDD', 'CDD'.
#' @param useEstimatedValues <boolean> describing if the estimated values of time series 
#' should be taken into account.
#' @param ratioCorrection <boolean> describing whether a ratio correction should be done, 
#' or not. Important to set to TRUE when time series contain gaps.
#' @return <data.frame> containing the resultant time series.

compute_device_aggregator_formula_v2 <- function(buildingsRdf, timeseriesObject, 
                                            buildingSpace, formula, 
                                            outputFrequency, aggFunctions,
                                            useEstimatedValues, ratioCorrection = F, minRatioCorrection=0.7,
                                            transformToAggregatableMeasuredProperty = F, 
                                            aggregatableMeasuredPropertyName = NULL,
                                            defaultFactorsByMeasuredProperty = NULL){
  
  result <- data.frame()
  op <- NULL
  tz <- get_tz_buildingSpace_v2(buildingsRdf, buildingSpace)
  while (formula!=""){
    if (substr(formula,1,4)=="<mi>"){
      res <- stringr::str_match(formula, "<mi>\\s*(.*?)\\s*</mi>")
      formula <- gsub(res[1,1],"",formula,fixed = T)
      aux_result <- eval(parse(text=
                                 paste0('get_sensor_v2(
            timeseriesObject = timeseriesObject,
            buildingsRdf = buildingsRdf,
            sensorId = "',res[1,2],'",
            tz = "',tz,'",
            outputFrequency = "',outputFrequency,'",
            aggFunctions = ',paste0('c("',paste(aggFunctions,collapse='","'),'")'),',
            useEstimatedValues = ',useEstimatedValues,',
            transformToAggregatableMeasuredProperty = transformToAggregatableMeasuredProperty,
            aggregatableMeasuredPropertyName = aggregatableMeasuredPropertyName,
            defaultFactorsByMeasuredProperty = defaultFactorsByMeasuredProperty
          )')
      ))
      if(is.null(aux_result)){return(NULL)}
      aux_result$utctime <- lubridate::with_tz(aux_result$time,"UTC")
      if(ratioCorrection){
        if(any(grepl("^SUM",colnames(aux_result)))){
          for (sum_col in colnames(aux_result)[grepl("^SUM",colnames(aux_result))]){
            aux_result[,sum_col] <- ifelse(aux_result$RATIO >= minRatioCorrection, 
                                           unlist(aux_result[,sum_col] / aux_result$RATIO), NA)
            aux_result <- aux_result[is.finite(unlist(aux_result[,sum_col])),]
          }
        }
      }
      if(length(result)==0){
        result <- aux_result
      } else {
        elems <- colnames(result)
        result$utctime <- lubridate::with_tz(result$time,"UTC")
        result <- merge(result %>% select(-time), aux_result %>% select(-time), 
                        suffixes=c("_1","_2"), all=T, 
                        by.x="utctime",by.y="utctime")
        result$time <- lubridate::with_tz(result$utctime,tz)
        result$utctime <- NULL
        elems <- elems[elems!="utctime"]
        if(is.null(op)) {
          stop("Device aggregator operator is not defined")
        } else if(op=="+") {
          for (elem in elems[!(elems %in% c("time"))]){
            if(grepl("^AVG|RATIO|GAPS",elem)){
              result[,elem] <- rowMeans(result[,c(paste0(elem,"_1"),paste0(elem,"_2"))],na.rm=T)
            } else {
              result[,elem] <- rowSums(result[,c(paste0(elem,"_1"),paste0(elem,"_2"))],na.rm=T)
            }
          }
        }
      }
      if(!is.null(result)){
        result[,endsWith(colnames(result),"_1") | endsWith(colnames(result),"_2")] <- NULL
      }
    } else if (substr(formula,1,4)=="<mo>"){
      res <- stringr::str_match(formula, "<mo>\\s*(.*?)\\s*</mo>")
      formula <- gsub(res[1,1],"",formula,fixed = T)
      op <- res[1,2]
    } else if (substr(formula,1,4)=="<mn>"){
      res <- stringr::str_match(formula, "<mn>\\s*(.*?)\\s*</mn>")
      formula <- gsub(res[1,1],"",formula,fixed = T)
      num <- res[1,2]
      for (elem in elems[!(elems %in% c("time"))]){
        result[,elem] <- eval(parse(text=paste0('result[,"',elem,'"] ',op,' num')))
      }
    }
  }
  return(result)
}



#' Compute the specified formula of a space property aggregator
#' 
#' This function obtains all the time series related with a device aggregator, aggregates them 
#' according to the device aggregator metadata and obtains a single resultant time series.
#' 
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param timeseriesObject <string> path of JSON files, or <list> of time series.
#' @param buildingSubject <uri> containing the building subject.
#' @param formula <string> describing the formula for the device aggregator. 
#' It consist on a sequence of arithmetical operations over one (or multiple) sensor 
#' identifier(s). The sensor identifiers must be written between prefix '<mi>' and suffix </mi>. 
#' On contrary, the operators are described between prefix <mo> and suffix </mo>.
#' Example of the sum of two sensors (let's identify them as 'ID1' and 'ID2'): 
#' '<mi>ID1</mi><mo>+</mo><mi>ID2</mi>'
#' @param outputFrequency <string> defining the frequency selected as output. 
#' It must follow ISO 8601 format representing the time step.
#' @param aggFunctions <string> describing the possible aggregation functions of the
#' resultant time series. Possible values: 'SUM', 'AVG', 'HDD', 'CDD'.
#' @param useEstimatedValues <boolean> describing if the estimated values of time series 
#' should be taken into account.
#' @param ratioCorrection <boolean> describing whether a ratio correction should be done, 
#' or not. Important to set to TRUE when time series contain gaps.
#' @return <data.frame> containing the resultant time series.

compute_space_property_aggregator_formula_v2 <- function(buildingsRdf, timeseriesObject, 
                                                 buildingSpace, formula, 
                                                 outputFrequency, aggFunctions,
                                                 useEstimatedValues, ratioCorrection = F, minRatioCorrection=0.7,
                                                 transformToAggregatableMeasuredProperty = F, 
                                                 aggregatableMeasuredPropertyName = NULL,
                                                 defaultFactorsByMeasuredProperty = NULL){
  
  result <- data.frame()
  op <- NULL
  tz <- get_tz_buildingSpace_v2(buildingsRdf, buildingSpace)
  while (formula!=""){
    if (substr(formula,1,4)=="<mi>"){
      res <- stringr::str_match(formula, "<mi>\\s*(.*?)\\s*</mi>")
      formula <- gsub(res[1,1],"",formula,fixed = T)
      aux_result <- eval(parse(text=
                                 paste0('get_space_measurement_v2(
            timeseriesObject = timeseriesObject,
            buildingsRdf = buildingsRdf,
            sensorId = "',res[1,2],'",
            tz = "',tz,'",
            outputFrequency = "',outputFrequency,'",
            aggFunctions = ',paste0('c("',paste(aggFunctions,collapse='","'),'")'),',
            useEstimatedValues = ',useEstimatedValues,',
            transformToAggregatableMeasuredProperty = transformToAggregatableMeasuredProperty,
            aggregatableMeasuredPropertyName = aggregatableMeasuredPropertyName,
            defaultFactorsByMeasuredProperty = defaultFactorsByMeasuredProperty
          )')
      ))
      if(is.null(aux_result)){return(NULL)}
      aux_result$utctime <- lubridate::with_tz(aux_result$time,"UTC")
      if(ratioCorrection){
        if(any(grepl("^SUM",colnames(aux_result)))){
          for (sum_col in colnames(aux_result)[grepl("^SUM",colnames(aux_result))]){
            aux_result[,sum_col] <- ifelse(aux_result$RATIO >= minRatioCorrection, 
                                           unlist(aux_result[,sum_col] / aux_result$RATIO), NA)
            aux_result <- aux_result[is.finite(unlist(aux_result[,sum_col])),]
          }
        }
      }
      if(length(result)==0){
        result <- aux_result
      } else {
        elems <- colnames(result)
        result$utctime <- lubridate::with_tz(result$time,"UTC")
        result <- merge(result %>% select(-time), aux_result %>% select(-time), 
                        suffixes=c("_1","_2"), all=T, 
                        by.x="utctime",by.y="utctime")
        result$time <- lubridate::with_tz(result$utctime,tz)
        result$utctime <- NULL
        elems <- elems[elems!="utctime"]
        if(is.null(op)) {
          stop("Device aggregator operator is not defined")
        } else if(op=="+") {
          for (elem in elems[!(elems %in% c("time"))]){
            if(grepl("^AVG|RATIO|GAPS",elem)){
              result[,elem] <- rowMeans(result[,c(paste0(elem,"_1"),paste0(elem,"_2"))],na.rm=T)
            } else {
              result[,elem] <- rowSums(result[,c(paste0(elem,"_1"),paste0(elem,"_2"))],na.rm=T)
            }
          }
        }
      }
      if(!is.null(result)){
        result[,endsWith(colnames(result),"_1") | endsWith(colnames(result),"_2")] <- NULL
      }
    } else if (substr(formula,1,4)=="<mo>"){
      res <- stringr::str_match(formula, "<mo>\\s*(.*?)\\s*</mo>")
      formula <- gsub(res[1,1],"",formula,fixed = T)
      op <- res[1,2]
    } else if (substr(formula,1,4)=="<mn>"){
      res <- stringr::str_match(formula, "<mn>\\s*(.*?)\\s*</mn>")
      formula <- gsub(res[1,1],"",formula,fixed = T)
      num <- res[1,2]
      for (elem in elems[!(elems %in% c("time"))]){
        result[,elem] <- eval(parse(text=paste0('result[,"',elem,'"] ',op,' num')))
      }
    }
  }
  return(result)
}

#' Get the metadata and time series from a filtered set of device aggregators
#' 
#' This function get the metadata and time series from a filtered set of device 
#' aggregators available in a BIGG-harmonised dataset. 
#' 
#' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' @param timeseriesObject <string> path of JSON files, or <list> of time series.
#' @param allowedBuildingSubjects <array> of URI(s) containing the allowed building 
#' subject(s).
#' @param allowedMeasuredProperties <array> of string(s) containing the allowed 
#' measured property(ies).
#' @param useEstimatedValues <boolean> describing if the estimated values of time 
#' series should be taken into account.
#' @param ratioCorrection <boolean> describing whether a ratio correction should 
#' be done, or not. Important to set to TRUE when time series contain gaps.
#' @param containsEEMs <boolean> to filter only those buildings that contain one
#' or more EEM.
#' @param alignGreaterThanHourlyFrequencyToYearly <boolean> to force time
#' alignment to P1Y frequency if original alignment frequency is greater to PT1H.
#' @param allowedSystems <character> to select the possible systems of the aggregated devices
#' potions are: "All: to get all the systems available or select one or a combination 
#' of the following, "GenerationSystem": Generation systems, "StorageSystem": Storage systems, 
#' "ConsumptionSystem: "Consumption systems, "GridSystem": Grid systems, "WeatherStationSystem": 
#' Weather Station systems. By default "All" is selected.
#' @return <list> of time series and metadata related with all device aggregators.

get_systems_device_aggregators_v2 <- function(
    buildingsRdf, timeseriesObject=NULL, allowedBuildingSpaces=NULL, 
    allowedMeasuredProperties=NULL, useEstimatedValues=F, ratioCorrection=T,
    containsEEMs=F, alignGreaterThanHourlyFrequencyToYearly=F,
    transformToAggregatableMeasuredProperty = F, aggregatableMeasuredPropertyName = NULL,
    measuredPropertiesToAggregate = NULL, defaultFactorsByMeasuredProperty = NULL, 
    allowedCommodities = NULL, allowedSystems="All"){
  
  # Get formulas and associated metadata for each building and device aggregator
  # ConsumptionSystem <- get_consumption_device_aggregators_metadata_v2
  # GridSystem <- get_grid_device_aggregators_metadata_v2
  # GenerationSystem <- get_generation_device_aggregators_metadata_v2
  # StorageSystem <- get_storage_device_aggregators_metadata_v2
  # WeatherStationSystem <- get_weather_device_aggregators_metadata_v2
  # 
  # 
  # agg_functions <- list("ConsumptionSystem"=ConsumptionSystem, 
  #                       "GridSystem"=GridSystem,
  #                       "GenerationSystem"=GenerationSystem,
  #                       "StorageSystem"=StorageSystem, 
  #                       "WeatherStationSystem"=WeatherStationSystem)
  
  
  if ((!length(allowedSystems)>1) && (allowedSystems == "All")) {
    #   devagg_buildings <- bind_rows(lapply(names(agg_functions), function(f) {
    #     res <- do.call(agg_functions[[f]], list(buildingsRdf))  
    #     res$type <- f
    #     return(res)
    # }))
    selectedSystems <- c("bigg:ConsumptionSystem", "bigg:GridSystem", "bigg:GenerationSystem", "bigg:StorageSystem", "s4agri:WeatherStation")
  } else {
    # devagg_buildings <- bind_rows(lapply(names(agg_functions[names(agg_functions) %in% allowedSystems]), function(f) {
    #   res <- do.call(agg_functions[[f]], list(buildingsRdf))  
    #   res$type <- f
    #   return(res)
    # }))
    selectedSystems <- unlist(lapply(allowedSystems, function(x) {
      if (x == "WeatherStationSystem") "s4agri:WeatherStation" else paste0("bigg:",x)
      }))
  }
  #Get selected selected device aggregators metadata
  devagg_buildings <- get_selected_device_aggregators_metadata_v2(buildingsRdf, selectedSystems)
    
  # Filter by the allowed buildings
  if(!is.null(allowedBuildingSpaces)){
    devagg_buildings <- devagg_buildings[
      devagg_buildings$buildingSpace %in% allowedBuildingSpaces,
    ]
  } else {
    allowedBuildingSpaces <- unique(devagg_buildings$buildingSpace)
  }
  
  # Filter by the allowed related properties
  if(!is.null(allowedMeasuredProperties)){
    devagg_buildings <- devagg_buildings[
      devagg_buildings$measuredProperty %in% allowedMeasuredProperties,
    ]
  }
  
  # Filter by the allowed systems commodities
  if(!is.null(allowedCommodities)){
    devagg_buildings <- devagg_buildings[
      devagg_buildings$deviceCommodity %in% c(allowedCommodities,"NotCommodity"),
    ]
  }
  
  
  
  # Filter only buildings that contains EEMs
  if(containsEEMs==T & nrow(devagg_buildings)>0){
    eems_buildings <- get_building_eems(buildingsRdf, unique(devagg_buildings$buildingSubject))
    eems_buildings <- eems_buildings %>% group_by(buildingSubject) %>% summarise(
      EEMexists = sum(is.character(eemSubject))>0
    ) %>% ungroup()
    devagg_buildings <- devagg_buildings %>% left_join(eems_buildings, by = "buildingSubject") %>% 
      filter(EEMexists)
  }
  
  # Get the data by building
  all_buildings_timeseries <- 
    setNames(lapply(unique(devagg_buildings$buildingSpace),
                    function(buildingSpace){
                      aux <- devagg_buildings[devagg_buildings$buildingSpace==buildingSpace,]
                      # Return NULL object if not sufficient measured properties are defined in device aggregators.
                      otherMeasuredProperties <- allowedMeasuredProperties[!(allowedMeasuredProperties %in% measuredPropertiesToAggregate)]
                      if (!all(otherMeasuredProperties %in% unique(aux$measuredProperty)) &&
                          !any(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))||(!nrow(aux)>0)){
                        write(sprintf(
                          "* Any device aggregator of building subject %s\n was related with the following needed measured properties: %s\n ",
                          buildingSpace,
                          paste(unique(c(
                            otherMeasuredProperties[!(otherMeasuredProperties %in% unique(aux$measuredProperty))],
                            measuredPropertiesToAggregate[!(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))]
                          )),collapse=", ")),stderr())
                        return(NULL)
                      }
                      if (!all(otherMeasuredProperties %in% unique(aux$measuredProperty)) ||
                          !any(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))){
                        write(sprintf(
                          "* Any device aggregator of building subject %s\n was related with the following needed measured properties: %s\n ",
                          buildingSpace,
                          paste(unique(c(
                            otherMeasuredProperties[!(otherMeasuredProperties %in% unique(aux$measuredProperty))],
                            measuredPropertiesToAggregate[!(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))]
                          )),collapse=", ")),stderr())
                      }
                      if(!all(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))){
                        write(sprintf(
                          "* Any device aggregator of building subject %s\n was related with the following needed measured properties: %s\n The analysis will continue only considering the other measured properties.",
                          buildingSpace,
                          paste(measuredPropertiesToAggregate[!(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))],
                          collapse=", ")),stderr())
                      }
                      aux$deviceAggregatorFrequency <- ifelse(
                        is.na(aux$deviceAggregatorFrequency), 
                        "P1M", aux$deviceAggregatorFrequency)
                      if(transformToAggregatableMeasuredProperty){
                        largerFrequencies <-
                          aux$deviceAggregatorFrequency[ aux$measuredProperty %in% measuredPropertiesToAggregate ]
                        largerFrequency <- largerFrequencies[
                            which.max(lubridate::seconds(lubridate::period(largerFrequencies)))]
                      } else {
                        largerFrequency <-
                          aux$deviceAggregatorFrequency[
                            which.max(lubridate::seconds(lubridate::period(aux$deviceAggregatorFrequency)))]
                        if(alignGreaterThanHourlyFrequencyToYearly && as.period(largerFrequency)>as.period("PT1H")){
                          largerFrequency <- "P1Y"
                        }
                      }
                      
                      dfs <- setNames(lapply(unique(aux$deviceAggregatorName),
                                             function(devAggName){
                                               #devAggName = "EnergyConsumptionGridElectricity"
                                               df <- compute_device_aggregator_formula_v2(
                                                 buildingsRdf = buildingsRdf,
                                                 timeseriesObject = timeseriesObject,
                                                 buildingSpace = buildingSpace,
                                                 formula = as.character(unique(aux[aux$deviceAggregatorName==devAggName,
                                                                                   "deviceAggregatorFormula"])),
                                                 outputFrequency = largerFrequency,
                                                 aggFunctions = unlist(
                                                   unique(aux[aux$deviceAggregatorName==devAggName,
                                                              "deviceAggregatorTimeAggregationFunction"]), use.names = F),
                                                 useEstimatedValues = useEstimatedValues,
                                                 ratioCorrection = ratioCorrection,
                                                 transformToAggregatableMeasuredProperty = 
                                                   if(devAggName %in% names(measuredPropertiesToAggregate)){
                                                     transformToAggregatableMeasuredProperty} else {F}, 
                                                 aggregatableMeasuredPropertyName = aggregatableMeasuredPropertyName,
                                                 defaultFactorsByMeasuredProperty = defaultFactorsByMeasuredProperty
                                               )
                                               if(is.null(df)){
                                                 return(NULL)
                                               } else {
                                                 colnames(df) <- ifelse(colnames(df)=="time","time",
                                                                        paste(devAggName, colnames(df), sep="."))
                                                 return(df)
                                               }
                                             }), nm = unique(aux$deviceAggregatorName))
                      dfs <- dfs[mapply(function(l)!is.null(l),dfs)]
                      if(is.null(dfs)) return(NULL)
                      tz <- lubridate::tz(dfs[[1]]$time)
                      dfs <- lapply(dfs,function(x)cbind(x,data.frame("utctime"=lubridate::with_tz(x$time,"UTC"))))
                      ldf <- list(
                        "df"=Reduce(function(df1, df2){merge(df1[!is.na(df1$utctime),!(colnames(df1)=="time")], 
                                                             df2[!is.na(df2$utctime),!(colnames(df2)=="time")], 
                                                             by = "utctime", all=T)}, dfs),
                        "metadata"=devagg_buildings[devagg_buildings$buildingSpace==buildingSpace,]
                      )
                      
                      # Aggregation of all the energy measured properties and convert them appropriately
                      if(transformToAggregatableMeasuredProperty){
                        colnames(ldf$df) <- gsub(paste0("^",names(measuredPropertiesToAggregate),collapse="|"), 
                                                 aggregatableMeasuredPropertyName, colnames(ldf$df))
                        ldf$df <- as.data.frame(
                          setNames(lapply(unique(colnames(ldf$df)),
                            function(x){
                              # IMPORTANT! na.rm is set to false because data from all the possible sources need to be 
                              # described, if not, the aggregation could generate misleading time series.
                              if(grepl(".AVG_|GAPS|RATIO",x) && !is.null(nrow(ldf$df[,which(colnames(ldf$df) %in% x)]))){
                                rowMeans(ldf$df[,which(colnames(ldf$df) %in% x)],na.rm=F)
                              } else if(grepl(".SUM_",x) && !is.null(nrow(ldf$df[,which(colnames(ldf$df) %in% x)]))){
                                rowSums(ldf$df[,which(colnames(ldf$df) %in% x)],na.rm=F)
                              } else {ldf$df[,which(colnames(ldf$df) %in% x)]}
                            }),unique(colnames(ldf$df))))
                      }
                      # plot(aaa$EnergyConsumptionTotal.SUM_EnergyConsumptionTotal,type="l")
                      # lines(ldf$df[,which(colnames(ldf$df) %in% "EnergyConsumptionTotal.SUM_EnergyConsumptionTotal")[1]],col="red")
                      # lines(ldf$df[,which(colnames(ldf$df) %in% "EnergyConsumptionTotal.SUM_EnergyConsumptionTotal")[2]],col="blue")
                      ldf$df[grepl(".utctime",colnames(ldf$df))] <- NULL
                      ldf$df$time <- lubridate::with_tz(ldf$df$utctime,tz)
                      
                      return(ldf)
                    }
    ), nm = unique(devagg_buildings$buildingSpace))
  
  return(all_buildings_timeseries)
}







get_buildingspace_aggregators_v2 <- function(
    buildingsRdf, timeseriesObject=NULL, allowedBuildingSpaces=NULL, 
    allowedMeasuredProperties=NULL, useEstimatedValues=F, ratioCorrection=T,
    containsEEMs=F, alignGreaterThanHourlyFrequencyToYearly=F,
    transformToAggregatableMeasuredProperty = F, aggregatableMeasuredPropertyName = NULL,
    measuredPropertiesToAggregate = NULL, defaultFactorsByMeasuredProperty = NULL, allowedInformation="All", getScenario=F){
  
  # Get formulas and associated metadata for each building and device aggregator
  Occupancy <- get_occupancy_metadata_v2
  SpaceState <- get_space_state_metadata_v2

  agg_functions <- list("Occupancy"=Occupancy, 
                        "SpaceState"=SpaceState)
  
  if (getScenario==T) {
    allowedMeasuredProperties = c(allowedMeasuredProperties, unlist(lapply(allowedMeasuredProperties, function (i) {paste0(i,"Scenario")})))
    measuredPropertiesToAggregate = c(measuredPropertiesToAggregate, unlist(lapply(measuredPropertiesToAggregate, function (i) {paste0(i,"Scenario")})))
    aggregatableMeasuredPropertyName = c(aggregatableMeasuredPropertyName, unlist(lapply(aggregatableMeasuredPropertyName, function (i) {paste0(i,"Scenario")})))
  }
  
  if (allowedInformation == "All") {
    devagg_buildings <- bind_rows(lapply(names(agg_functions), function(f) {
      res <- do.call(agg_functions[[f]], list(buildingsRdf))  
      res$type <- f
      return(res)
    }))
  } else {
    devagg_buildings <- bind_rows(lapply(names(agg_functions[names(agg_functions) %in% allowedInformation]), function(f) {
      res <- do.call(agg_functions[[f]], list(buildingsRdf))  
      res$type <- f
      return(res)
    }))
  }
  
  # devagg_buildings <- get_device_aggregators_metadata(buildingsRdf)
  
  # Filter by the allowed buildings
  if(!is.null(allowedBuildingSpaces)){
    devagg_buildings <- devagg_buildings[
      devagg_buildings$buildingSpace %in% allowedBuildingSpaces,
    ]
  } else {
    allowedBuildingSpaces <- unique(devagg_buildings$buildingSpace)
  }
  
  # Filter by the allowed device aggregator names
  if(!is.null(allowedMeasuredProperties)){
    devagg_buildings <- devagg_buildings[
      devagg_buildings$measuredProperty %in% allowedMeasuredProperties,
    ]
  }
  
  # Filter only buildings that contains EEMs
  if(containsEEMs==T & nrow(devagg_buildings)>0){
    eems_buildings <- get_building_eems(buildingsRdf, unique(devagg_buildings$buildingSubject))
    eems_buildings <- eems_buildings %>% group_by(buildingSubject) %>% summarise(
      EEMexists = sum(is.character(eemSubject))>0
    ) %>% ungroup()
    devagg_buildings <- devagg_buildings %>% left_join(eems_buildings, by = "buildingSubject") %>% 
      filter(EEMexists)
  }
  
  # Get the data by building
  all_buildings_timeseries <- 
    setNames(lapply(unique(devagg_buildings$buildingSpace),
                    function(buildingSpace){
                      aux <- devagg_buildings[devagg_buildings$buildingSpace==buildingSpace,]
                      # Return NULL object if not sufficient measured properties are defined in device aggregators.
                      otherMeasuredProperties <- allowedMeasuredProperties[!(allowedMeasuredProperties %in% measuredPropertiesToAggregate)]
                      if (!all(otherMeasuredProperties %in% unique(aux$measuredProperty)) &&
                          !any(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))||(!nrow(aux)>0)){
                        write(sprintf(
                          "* Any device aggregator of building subject %s\n was related with the following needed measured properties: %s\n ",
                          buildingSpace,
                          paste(unique(c(
                            otherMeasuredProperties[!(otherMeasuredProperties %in% unique(aux$measuredProperty))],
                            measuredPropertiesToAggregate[!(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))]
                          )),collapse=", ")),stderr())
                        return(NULL)
                      }
                      if (!all(otherMeasuredProperties %in% unique(aux$measuredProperty)) ||
                          !any(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))){
                        write(sprintf(
                          "* Any device aggregator of building subject %s\n was related with the following needed measured properties: %s\n ",
                          buildingSpace,
                          paste(unique(c(
                            otherMeasuredProperties[!(otherMeasuredProperties %in% unique(aux$measuredProperty))],
                            measuredPropertiesToAggregate[!(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))]
                          )),collapse=", ")),stderr())
                      }
                      if(!all(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))){
                        write(sprintf(
                          "* Any device aggregator of building subject %s\n was related with the following needed measured properties: %s\n The analysis will continue only considering the other measured properties.",
                          buildingSpace,
                          paste(measuredPropertiesToAggregate[!(measuredPropertiesToAggregate %in% unique(aux$measuredProperty))],
                                collapse=", ")),stderr())
                      }
                      aux$deviceAggregatorFrequency <- ifelse(
                        is.na(aux$deviceAggregatorFrequency), 
                        "P1M", aux$deviceAggregatorFrequency)
                      if(transformToAggregatableMeasuredProperty){
                        largerFrequencies <-
                          aux$deviceAggregatorFrequency[ aux$measuredProperty %in% measuredPropertiesToAggregate ]
                        largerFrequency <- largerFrequencies[
                          which.max(lubridate::seconds(lubridate::period(largerFrequencies)))]
                      } else {
                        largerFrequency <-
                          aux$deviceAggregatorFrequency[
                            which.max(lubridate::seconds(lubridate::period(aux$deviceAggregatorFrequency)))]
                        if(alignGreaterThanHourlyFrequencyToYearly && as.period(largerFrequency)>as.period("PT1H")){
                          largerFrequency <- "P1Y"
                        }
                      }
                      
                      dfs <- setNames(lapply(unique(aux$measuredProperty),
                                             function(devAggName){
                                               #devAggName = "Occupancy"
                                               df <-  compute_space_property_aggregator_formula_v2(
                                                 buildingsRdf = buildingsRdf,
                                                 timeseriesObject = timeseriesObject,
                                                 buildingSpace = buildingSpace,
                                                 formula = as.character(unique(aux[aux$measuredProperty==devAggName,
                                                                                   "deviceAggregatorFormula"])),
                                                 outputFrequency = largerFrequency,
                                                 aggFunctions = unlist(
                                                   unique(aux[aux$measuredProperty==devAggName,
                                                              "deviceAggregatorTimeAggregationFunction"]), use.names = F),
                                                 useEstimatedValues = useEstimatedValues,
                                                 ratioCorrection = ratioCorrection,
                                                 transformToAggregatableMeasuredProperty = 
                                                   if(devAggName %in% names(measuredPropertiesToAggregate)){
                                                     transformToAggregatableMeasuredProperty} else {F}, 
                                                 aggregatableMeasuredPropertyName = aggregatableMeasuredPropertyName,
                                                 defaultFactorsByMeasuredProperty = defaultFactorsByMeasuredProperty
                                               )
                                               if(is.null(df)){
                                                 return(NULL)
                                               } else {
                                                 colnames(df) <- ifelse(colnames(df)=="time","time",
                                                                        paste(devAggName, colnames(df), sep="."))
                                                 return(df)
                                               }
                                             }), nm = unique(aux$measuredProperty))
                      dfs <- dfs[mapply(function(l)!is.null(l),dfs)]
                      if(is.null(dfs)) return(NULL)
                      tz <- lubridate::tz(dfs[[1]]$time)
                      dfs <- lapply(dfs,function(x)cbind(x,data.frame("utctime"=lubridate::with_tz(x$time,"UTC"))))
                      ldf <- list(
                        "df"=Reduce(function(df1, df2){merge(df1[!is.na(df1$utctime),!(colnames(df1)=="time")], 
                                                             df2[!is.na(df2$utctime),!(colnames(df2)=="time")], 
                                                             by = "utctime", all=T)}, dfs),
                        "metadata"=devagg_buildings[devagg_buildings$buildingSpace==buildingSpace,]
                      )
                      
                      # Aggregation of all the energy measured properties and convert them appropriately
                      if(transformToAggregatableMeasuredProperty){
                        colnames(ldf$df) <- gsub(paste0("^",names(measuredPropertiesToAggregate),collapse="|"), 
                                                 aggregatableMeasuredPropertyName, colnames(ldf$df))
                        ldf$df <- as.data.frame(
                          setNames(lapply(unique(colnames(ldf$df)),
                                          function(x){
                                            # IMPORTANT! na.rm is set to false because data from all the possible sources need to be 
                                            # described, if not, the aggregation could generate misleading time series.
                                            if(grepl(".AVG_|GAPS|RATIO",x) && !is.null(nrow(ldf$df[,which(colnames(ldf$df) %in% x)]))){
                                              rowMeans(ldf$df[,which(colnames(ldf$df) %in% x)],na.rm=F)
                                            } else if(grepl(".SUM_",x) && !is.null(nrow(ldf$df[,which(colnames(ldf$df) %in% x)]))){
                                              rowSums(ldf$df[,which(colnames(ldf$df) %in% x)],na.rm=F)
                                            } else {ldf$df[,which(colnames(ldf$df) %in% x)]}
                                          }),unique(colnames(ldf$df))))
                      }
                      # plot(aaa$EnergyConsumptionTotal.SUM_EnergyConsumptionTotal,type="l")
                      # lines(ldf$df[,which(colnames(ldf$df) %in% "EnergyConsumptionTotal.SUM_EnergyConsumptionTotal")[1]],col="red")
                      # lines(ldf$df[,which(colnames(ldf$df) %in% "EnergyConsumptionTotal.SUM_EnergyConsumptionTotal")[2]],col="blue")
                      ldf$df[grepl(".utctime",colnames(ldf$df))] <- NULL
                      ldf$df$time <- lubridate::with_tz(ldf$df$utctime,tz)
                      
                      return(ldf)
                    }
    ), nm = unique(devagg_buildings$buildingSpace))
  
  return(all_buildings_timeseries)
}



#' 
#' 
#' #
#' # Read Energy Efficiency Measures (EEMs) ----
#' #
#' 
#' #' Check if exists a project.
#' #' 
#' #' This function checks if a certain project subject exists in a BIGG-harmonised dataset.
#' #'
#' #' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' #' @param projectSubject <uri> of project subject.
#' #' @param namespaces named <array> that relates simple namespaces and complete 
#' #' ones.
#' #' @return <boolean> if the project exists.
#' 
#' exists_project_model <- function(buildingsRdf, projectSubject, namespaces){
#'   projectSubject <- namespace_integrator(projectSubject, namespaces)
#'   return(nrow(suppressMessages(buildingsRdf %>% rdf_query(paste0(    
#'     paste0(mapply(function(i){
#'       sprintf('PREFIX %s: <%s>', names(bigg_namespaces_v2)[i],
#'               bigg_namespaces_v2[i])},
#'       1:length(bigg_namespaces_v2))),
#'     '
#'     SELECT ?m
#'     WHERE {
#'       ?m a bigg:Project .
#'       FILTER (?m = <',projectSubject,'>) .
#'     }'))))>0)
#' }
#' 
#' #' Get the Energy Efficiency Measures (EEMs) subjects from a set of buildings
#' #' 
#' #' This function search for all the available EEMs subjects
#' #' in a set of buildings. It also relates the EEMs with the 
#' #' building element.
#' #'
#' #' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' #' @param buildingSubjects <array> of URIs related with the building subjects.
#' #' @return <data.frame> containing the building subject, building element and 
#' #' EEM subject.
#' 
#' get_building_eems <- function(buildingsRdf, buildingSubjects=NULL){
#'   result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(
#'     paste0(mapply(function(i){
#'       sprintf('PREFIX %s: <%s>', names(bigg_namespaces_v2)[i],
#'               bigg_namespaces_v2[i])},
#'       1:length(bigg_namespaces_v2))),
#'     '
#'     SELECT ?buildingSubject ?buildingElement ?eemSubject
#'     WHERE {
#'       {
#'         SELECT ?buildingSubject ?buildingElement ?eemSubject
#'         WHERE {
#'           ?buildingSubject a bigg:Building .',
#'     ifelse(!is.null(buildingSubjects),paste0('
#'             FILTER ( ?buildingSubject IN (<',paste(buildingSubjects,collapse='>,<'),'>) ) .'),
#'            ''),
#'     '?buildingSubject bigg:hasSpace ?bs .
#'           ?bs bigg:isAssociatedWithElement ?buildingElement .
#'         }
#'       }
#'       optional {?buildingElement bigg:isAffectedByMeasure ?eemSubject .}
#'     }')))
#'   return(if(length(result)>0) {
#'     result
#'   } else {NULL})
#' }
#' 
#' #' Get the complete metadata from a set of Energy Efficiecy Measures (EEMs)
#' #' 
#' #' This function get the metadata of a set of EEMs in a BIGG-harmonised dataset. 
#' #' The investment cost is always converted to Euros, for benchmarking purposes.
#' #'
#' #' @param buildingsRdf <rdf> containing the information of a set of buildings.
#' #' @param eemSubjects <array> of URIs related with the EEM subjects.
#' #' @return <data.frame> containing the metadata of EEMs
#' 
#' get_eem_details <- function(buildingsRdf, eemSubjects=NULL){
#'   result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(
#'     paste0(mapply(function(i){
#'       sprintf('PREFIX %s: <%s>', names(bigg_namespaces_v2)[i],
#'               bigg_namespaces_v2[i])},
#'       1:length(bigg_namespaces_v2))),
#'     '
#'     SELECT ?eemSubject ?ExchangeRate ?Description ?Investment ?DateEnd ?DateStart ?Currency ?Type ?AffectationShare
#'     WHERE {
#'       ?eemSubject a bigg:EnergyEfficiencyMeasure .',
#'     ifelse(!is.null(eemSubjects),paste0('
#'             FILTER ( ?eemSubject IN (<',paste(eemSubjects,collapse='>,<'),'>) ) .'),'
#'             '),
#'     'optional { ?eemSubject bigg:energyEfficiencyMeasureCurrencyExchangeRate ?ExchangeRate .}
#'        optional { ?eemSubject bigg:energyEfficiencyMeasureDescription ?Description .}
#'        optional { ?eemSubject bigg:energyEfficiencyMeasureInvestment ?Investment .}
#'        optional { ?eemSubject bigg:energyEfficiencyMeasureOperationalDate ?DateEnd .}
#'        optional { ?eemSubject bigg:startWorkDate ?DateStart .}
#'        optional { ?eemSubject bigg:hasEnergyEfficiencyMeasureInvestmentCurrency ?Currency .}
#'        optional { ?eemSubject bigg:hasEnergyEfficiencyMeasureType ?Type .}
#'        optional { ?eemSubject bigg:shareOfAffectedElement ?AffectationShare .}
#'     }')))
#'   result$Investment <- ifelse(is.finite(result$ExchangeRate),
#'                               result$Investment * result$ExchangeRate,
#'                               result$Investment)
#'   result$Currency <- ifelse(is.finite(result$ExchangeRate),
#'                             "http://qudt.org/vocab/unit/Euro",
#'                             result$Currency)
#'   
#'   return(if(nrow(result)>0) {
#'     result
#'   } else {NULL})
#' }
#' 
#' get_eem_projects <- function(buildingsRdf, buildingSubject, eemSubjects=NULL){
#'   result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(
#'     paste0(mapply(function(i){
#'       sprintf('PREFIX %s: <%s>', names(bigg_namespaces_v2)[i],
#'               bigg_namespaces_v2[i])},
#'       1:length(bigg_namespaces_v2))),
#'     # '
#'     # SELECT ?eemProjectSubject ?buildingSubject ?eemSubject ?Description ?Investment ?DateStart ?DateEnd
#'     # WHERE {
#'     #   ?eemProjectSubject a bigg:Project .
#'     #   ?eemProjectSubject bigg:affectsBuilding ?buildingSubject .',
#'     #   paste0('FILTER ( ?buildingSubject IN (<',paste(buildingSubject,collapse='>,<'),'>) ) .'),'
#'     #   ?eemProjectSubject bigg:includesMeasure ?eemSubject .',
#'     #   ifelse(!is.null(eemSubjects),paste0('
#'     #         FILTER ( ?eemSubject IN (<',paste(eemSubjects,collapse='>,<'),'>) ) .'),'
#'     #         '),
#'     #   'optional { ?eemProjectSubject bigg:projectDescription ?Description .}
#'     #    optional { ?eemProjectSubject bigg:projectInvestment ?Investment .}
#'     #    optional { ?eemProjectSubject bigg:projectOperationalDate ?DateEnd .}
#'     #    optional { ?eemProjectSubject bigg:projectStartDate ?DateStart .}
#'     # }'
#'     '
#'     SELECT ?eemProjectSubject ?buildingSubject ?eemSubject ?Description ?Investment ?DateStart ?DateEnd
#'     WHERE {
#'       {
#'         SELECT ?buildingSubject ?eemProjectSubject
#'         WHERE {
#'           ?buildingSubject a bigg:Building .',
#'           paste0('FILTER ( ?buildingSubject IN (<',paste(buildingSubject,collapse='>,<'),'>) ) .'),'
#'           ?buildingSubject bigg:hasProject ?eemProjectSubject .
#'         }
#'       }
#'       ?eemProjectSubject bigg:includesMeasure ?eemSubject .',
#'       ifelse(!is.null(eemSubjects),paste0('
#'             FILTER ( ?eemSubject IN (<',paste(eemSubjects,collapse='>,<'),'>) ) .'),'
#'             '),
#'       'optional { ?eemProjectSubject bigg:projectDescription ?Description .}
#'        optional { ?eemProjectSubject bigg:projectInvestment ?Investment .}
#'        optional { ?eemProjectSubject bigg:projectOperationalDate ?DateEnd .}
#'        optional { ?eemProjectSubject bigg:projectStartDate ?DateStart .}
#'     }'
#'     )))
#'   if(nrow(result)>0) {
#'     result$eemProjectId <- factor(result$eemProjectSubject, levels=unique(result$eemProjectSubject), 
#'                                   labels=c(1:length(unique(result$eemProjectSubject))))
#'     return(result)
#'   } else {return(NULL)}
#' }
#' 
#' #
#' # Read Key Performance Indicators (KPIs) ----
#' #
#' 
#' #' Get one Key Performance Indicator (KPI) time series from a certain building.
#' #' 
#' #' This function use the harmonised data to extract a specific KPI time series of a certain 
#' #' building.
#' #'
#' #' @param buildingsRdf <rdf> containing all metadata about buildings. 
#' #' It must be harmonised to BIGG Ontology.
#' #' @param timeseriesObject <array> of strings with paths to JSON files containing time series,
#' #' or <list> of time series. It must be harmonised to BIGG Ontology.
#' #' @param buildingSubject <uri> of the building subject in buildingsRdf.
#' #' @param name <string> defining the indicator name to be retrieved. 
#' #' @param fromModel <boolean> defining if the time series should be real (FALSE), or estimated (TRUE)
#' #' @param frequency <string> defining the frequency to be retrieved. 
#' #' It must follow ISO 8601 format representing the time step. 
#' #' Examples: 'P1D' (One day), P1Y' (One year), 'P1M' (One month)
#' #' @return <data.frame> with columns 'time' and 'value'.
#' 
#' get_KPI_timeseries <- function(buildingsRdf, timeseriesObject, buildingSubject, 
#'                                name, fromModel, frequency){
#'   
#'   KPI_metadata <- suppressMessages(buildingsRdf %>% rdf_query(paste0(
#'     paste0(mapply(function(i){
#'       sprintf('PREFIX %s: <%s>', names(bigg_namespaces_v2)[i],
#'               bigg_namespaces_v2[i])},
#'       1:length(bigg_namespaces_v2))),
#'     '
#'     SELECT ?SingleKPI ?uriTimeSeries ?date
#'     WHERE {
#'       {
#'         SELECT ?SingleKPI ?uriTimeSeries
#'         WHERE {
#'           <',buildingSubject,'> bigg:assessesSingleKPI ?SingleKPI .
#'           FILTER regex(str(?SingleKPI),"',name,'") .
#'           ?SingleKPI bigg:timeSeriesFrequency "',frequency,'".
#'           ?SingleKPI bigg:hasSingleKPIPoint ?uriTimeSeries .
#'         }
#'       }
#'       OPTIONAL {?SingleKPI bigg:isEstimatedByModel ?est .}
#'       FILTER (',if(fromModel){""}else{"!"},'BOUND(?est))
#'       {
#'         OPTIONAL {?est bigg:modelTrainedDate ?date .}
#'       } UNION {
#'         BIND("1970-01-01T00:00:00.000" as ?date)
#'       }
#'     }
#'     ORDER BY DESC(?date) LIMIT 1
#'   ')))
#'   
#' 
#'   
#'   if(nrow(KPI_metadata)==0) return(NULL)
#'   
#'   KPI_metadata$uriTimeSeries <- mapply(
#'     function(i){i[2]},
#'     strsplit(KPI_metadata$uriTimeSeries,"#"))
#'   
#'   if(is.character(timeseriesObject)){
#'     timeseriesObject_ <- unlist(lapply(
#'       timeseriesObject[grepl(KPI_metadata$uriTimeSeries[1],timeseriesObject)],
#'       function(x){jsonlite::fromJSON(x)}),recursive=F)
#'   } else {
#'     timeseriesObject_ <- timeseriesObject[[KPI_metadata$uriTimeSeries[1]]]
#'   }
#'   
#'   timeseriesKPI <- timeseriesObject_[[KPI_metadata$uriTimeSeries[1]]]
#'   timeseriesKPI$start <- parse_iso_8601(timeseriesKPI$start)
#'   timeseriesKPI$end <- parse_iso_8601(timeseriesKPI$end)
#'   timeseriesKPI <- timeseriesKPI[order(timeseriesKPI$start),]
#'   timeseriesKPI <- timeseriesKPI %>% filter(is.finite(value))
#'   timeseriesKPI$time <- timeseriesKPI$start
#'   timeseriesKPI$start <- timeseriesKPI$end <- timeseriesKPI$isReal <- NULL
#'   
#'   return(timeseriesKPI)
#' }

#
# Read energy tariffs and emissions data ----
#

#' Get the energy tariff metadata
#' 
#' This function extract the energy tariff metadata related to a certain sensor identifier.
#'
#' @param buildingsRdf <rdf> containing all metadata about buildings. 
#' It must be harmonised to BIGG Ontology.
#' @param sensorId <string> with the sensor identifier 
#' (e.g. related to some consumption time series).
#' @return <data.frame> with the energy tariffs metadata.

get_tariff_metadata_v2 <- function(buildingsRdf, measurementId){
  gridsystems <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
  SELECT ?gs ?tariff
  WHERE {
     ?gs a bigg:GridSystem .
     ?gs bigg:hasContractedTariff ?tariff .
  }')))
  
  for (i in 1:nrow(gridsystems)) {
    gs <- c(gridsystems[i,]$gs, find_related_subjects_per_type(buildingsRdf,gridsystems$gs,"s4syst:hasSubSystem", "s4syst:System"))
    result <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
      set_query_prefixes_v2(namespaces),
      '
      SELECT ?system ?hasMeasurement 
      WHERE {
        {
            ?system a s4syst:System .
            FILTER ( ?system IN (<',paste(gs,collapse='>,<'),'>) ) .
            ?system s4syst:hasSubSystem ?device .
            ?device a saref:Device.
            ?device saref:makesMeasurement ?hasMeasurement .
            ?hasMeasurement bigg:hash ?hash .
            FILTER regex(str(?hash), "',measurementId,'")
        }
      }')))
    if(nrow(result)>0) {
      metadata_df <- gridsystems[i,]
      metadata_df$measurementId <- measurementId
    } else{metadata_df <- tibble(gs=character(0),
                                tariff=character(0),
                                measurementId=character(0)
                                )}
  }
  return(metadata_df)
}

#' Get the emissions metadata
#' 
#' This function extract the emissions metadata related to a certain sensor identifier.
#'
#' @param buildingsRdf <rdf> containing all metadata about buildings. 
#' It must be harmonised to BIGG Ontology.
#' @param sensorId <string> with the sensor identifier 
#' (e.g. related to some consumption time series).
#' @return <data.frame> with the emissions metadata.

get_emissions_metadata_v2 <- function(buildingsRdf, sensorId, deviceName, deviceCommodity){
  fcodes <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT  ?fc ?emissions ?nm
    WHERE {
      ?fc bigg:hasCO2EmissionsFactor ?emissions .
      ?emissions saref:hasMeasurement ?meas .
      ?meas bee:relatestoDeviceName ?nm .
      ?fc a gn:feature .
      FILTER regex(str(?nm), "',deviceName,'") .
    }')))
  if (nrow(fcodes)>0) {
    for( n in 1:nrow(fcodes)) {
      childs <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
        set_query_prefixes_v2(namespaces),
        '
      SELECT  ?fc 
      WHERE {
        ?fc a gn:feature .
        ?fc gn:parentFeature ?fcc
        FILTER ( ?fcc IN (<',paste(fcodes$fc,collapse='>,<'),'>) ) .
      }')))
      for (c in 1:nrow(childs))
      if (nrow(childs<0)) {fcodes <- fcodes %>% bind_rows(fcodes[n,] %>% mutate(fc=paste0(childs[c,]$fc)))}
    }
    fcodes <- fcodes[!duplicated(fcodes),]
    fcodes <- fcodes %>% mutate(bs = NA)
    for (fcod in unique(fcodes$fc)) {
      fcodes_contains <- find_all_related_subjects(buildingsRdf,fcod,"geosp:sfContains")
      if (length(fcodes_contains)>0) {
        for (fcb in fcodes_contains) { fcodes <- fcodes %>% bind_rows(fcodes[fcodes$fc==fcod & is.na(fcodes$bs),] %>% mutate(bs=fcb))}}
    }
    for (nr in 1:nrow(fcodes)) {
      fcodes[fcodes$bs %in% get_all_buldingspace_subspaces_v2(buildingsRdf, fcodes[nr,]$bs)[[fcodes[nr,]$bs]],]$bs<-NA
      
    }
    metadata_df <- fcodes[!is.na(fcodes$bs),]
    # metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    #   set_query_prefixes_v2(namespaces),
    #   '
    #   SELECT  ?fc ?bs
    #   WHERE {
    #     ?fc geosp:sfContains ?bs .
    #     ?fc a gn:feature .
    #     FILTER ( ?fc IN (<',paste(fcodes$fc,collapse='>,<'),'>) ) .
    #   }'))) %>% inner_join(fcodes, by="fc")
    # 
    for (i in 1:nrow(metadata_df)) {
      syst <- unlist(get_buildingspaces_systems_list_v2(buildingsRdf, find_related_subjects_per_type(buildingsRdf, metadata_df[i,]$bs, "geosp:sfContains", "s4bldg:BuildingSpace")), use.names=FALSE)
      if (!length(syst)>0) {
        metadata_df[i,]$emissions=NA
      } else{
        meas <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
          set_query_prefixes_v2(namespaces),
          '
      SELECT ?s ?nm ?hasMeasurement ?hash
      WHERE {
        ?s a s4syst:System.
        FILTER ( ?s IN (<',paste(syst,collapse='>,<'),'>) ) .
        ?s foaf:name ?nm .
        FILTER regex(str(?nm), "',deviceName,'") .
        ?s saref:makesMeasurement ?hasMeasurement .
        ?hasMeasurement bigg:hash ?hash .
        FILTER regex(str(?hash), "',sensorId,'")
      }')))
      if (nrow(meas)<1 || !(metadata_df[i,]$nm %in% meas$nm)) {
        metadata_df[i,]$emissions=NA
      }
    }
    
  }
  if (nrow(metadata_df)>1) {
    related <- character(0)
    for (nr in 1:nrow(metadata_df)) {
      related <- c(related,find_related_subjects_per_type(buildingsRdf, metadata_df[1,]$fc, "gn:parentFeature", "gn:feature"))
    }
    metadata_df[metadata_df$fc %in% unique(related),]$emissions=NA
  }
  metadata_df <- metadata_df[!(is.na(metadata_df$emissions)),]
  metadata_df$sensorId <- sensorId
  } else {metadata_df=tibble(emissions=character(0))}
  return(metadata_df)
}

#' Append the cost and price to some energy time series sensor
#' 
#' This function calculates the cost and append the price to some 
#' energy time series sensor, based on a tariff definition for that
#' sensor in a BIGG-harmonised dataset.
#'
#' @param buildingsRdf <rdf> containing all metadata about buildings. 
#' It must be harmonised to BIGG Ontology.
#' @param timeseriesObject <string> path of JSON files, or <list> of time 
#' series.
#' @param tariffSubject <uri> with the tariff identifier.
#' @param measuredProperty <string> with the energy measured property to 
#' consider for the tariff cost calculation 
#' (e.g. EnergyConsumptionGridElectricity, EnergyConsumptionGas)
#' @param frequency <string> defining the frequency to be considered in the 
#' energy cost calculation. It must follow ISO 8601 format representing the 
#' time step.
#' @param energyTimeseriesSensor <data.frame> output from 
#' get_sensor_v2().
#' 
#' @return <data.frame>, by-passing the input argument energyTimeseriesSensor and 
#' appending columns related to energy cost.

append_cost_to_sensor_v2 <- function(buildingsRdf, timeseriesObject, tariffSubject, measuredProperty,
                                frequency, energyTimeseriesSensor){
  tariffSubject <- namespace_integrator(tariffSubject,bigg_namespaces_v2)
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?measuredProperty ?timeSeriesFrequency ?hash ?start ?end
    WHERE {
      {
        SELECT ?ct 
        WHERE {
          ?ct a bigg:ContractedTariff .
          FILTER (?ct IN (<',paste0(tariffSubject,collapse='>,<'),'>)) .
        }
      }
      ?ct bigg:hasTariff ?tar .
      ?ct bigg:contractStartDate ?start .
      optional { ?ct bigg:contractEndDate ?end . }
      ?tar saref:hasMeasurement ?tcl.
      ?tcl saref:relatesToProperty ?measuredProperty.
      ?tcl bigg:measurementFrequency ?timeSeriesFrequency.
      ?tcl bigg:hash ?hash.
    }')))
  if(nrow(metadata_df)==0){
    return(energyTimeseriesSensor)
  } else {
    metadata_df$measuredProperty <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                         metadata_df$measuredProperty)
    
    if(any(metadata_df$timeSeriesFrequency==frequency)){
      metadata_df <- metadata_df[metadata_df$measuredProperty==measuredProperty &
                                   metadata_df$timeSeriesFrequency==frequency,]
    } else {
      frequency_ <- unique(metadata_df$timeSeriesFrequency[
        (as.period(metadata_df$timeSeriesFrequency) < as.period(frequency))])
      frequency_ <- frequency_[which.max(as.numeric(as.period(frequency_)))]
      metadata_df <- metadata_df[metadata_df$measuredProperty==measuredProperty &
                                   as.period(metadata_df$timeSeriesFrequency)==as.period(frequency_),]
    }
    metadata_df <- metadata_df[order(metadata_df$start),]
    prices <- do.call(rbind,lapply(1:nrow(metadata_df), function(i){
      
      if(!is.list(timeseriesObject)){
        timeseriesObject_ <- get_measurement_file_v2(timeseriesObject,metadata_df$hash[i]) 
      } else {
        timeseriesObject_ <- timeseriesObject[names(timeseriesObject) %in% metadata_df$hash[i]]
      }
      if(length(timeseriesObject_)==0){
        stop(sprintf("Measurement Hash %s (energy prices of %s) is not available in the time series of the building.",
                     metadata_df$hash[i], metadata_df$measuredProperty[i]))
      }
      timeseriesTariff <- timeseriesObject_[metadata_df$hash[i]][[1]]
      timeseriesTariff$start <- parse_iso_8601(timeseriesTariff$start)
      timeseriesTariff$end <- parse_iso_8601(timeseriesTariff$end)
      timeseriesTariff <- timeseriesTariff[order(timeseriesTariff$start),]
      timeseriesTariff <- timeseriesTariff[
        timeseriesTariff$start > metadata_df$start[i] & 
          if(is.finite(metadata_df$end[i])){
            timeseriesTariff$end < metadata_df$end[i]
          } else {T},
      ]}))
    prices <- prices[!duplicated(prices$start,fromLast=T),]
    prices <- align_time_grid(
      data = prices,
      timeColumn = "start",
      outputFrequency = frequency,
      aggregationFunctions = "AVG",
      aggregationFunctionsSuffix = paste0(measuredProperty,"_EnergyPrice"),
      tz = lubridate::tz(energyTimeseriesSensor$time))
    prices <- prices %>% select(
      "time",
      paste0("AVG_",measuredProperty,"_EnergyPrice"))
    energyTimeseriesSensor <- energyTimeseriesSensor %>% 
      left_join(prices, by="time")
    energyTimeseriesSensor[,"SUM_EnergyCost"] <- 
      if(sum(grepl(paste0(measuredProperty,"$"),colnames(energyTimeseriesSensor)),na.rm=T)>1){
        rowSums(
          energyTimeseriesSensor[,grepl(paste0(measuredProperty,"$"),colnames(energyTimeseriesSensor))] *
            energyTimeseriesSensor[,paste0("AVG_",measuredProperty,"_EnergyPrice")], 
          na.rm=T)
      } else {
        energyTimeseriesSensor[,grepl(paste0(measuredProperty,"$"),colnames(energyTimeseriesSensor))] *
          energyTimeseriesSensor[,paste0("AVG_",measuredProperty,"_EnergyPrice")]
      }
    return(energyTimeseriesSensor)
  }
}

#' Append the emissions to some energy time series sensor
#' 
#' This function calculates the emissions related to some 
#' energy time series sensor, based on a tariff definition for that
#' sensor in a BIGG-harmonised dataset.
#'
#' @param buildingsRdf <rdf> containing all metadata about buildings. 
#' It must be harmonised to BIGG Ontology.
#' @param timeseriesObject <string> path of JSON files, or <list> of time 
#' series.
#' @param emissionsSubject <uri> with the emissions identifier.
#' @param measuredProperty <string> with the energy measured property to 
#' consider for the emissions calculation 
#' (e.g. EnergyConsumptionGridElectricity, EnergyConsumptionGas)
#' @param frequency <string> defining the frequency to be considered in the 
#' emissions calculation. It must follow ISO 8601 format representing the 
#' time step.
#' @param energyTimeseriesSensor <data.frame> output from 
#' get_sensor_v2().
#' 
#' @return <data.frame>, by-passing the input argument energyTimeseriesSensor 
#' and appending columns related to emissions.

append_emissions_to_sensor_v2 <- function(buildingsRdf, timeseriesObject, emissionsSubject, measuredProperty,
                                       deviceName, deviceCommodity, frequency, energyTimeseriesSensor){
  emissionsSubject <- namespace_integrator(emissionsSubject,bigg_namespaces_v2)
  metadata_df <- suppressMessages(buildingsRdf %>% rdf_query(paste0(    
    set_query_prefixes_v2(namespaces),
    '
    SELECT ?timeSeriesFrequency ?deviceName ?deviceCommodity ?hash
    WHERE {
      {
        SELECT ?em
        WHERE {
          ?em a bigg:CO2EmissionsFactor .
          FILTER (?em IN (<',paste0(emissionsSubject,collapse='>,<'),'>)) .
        }
      }
      ?em saref:hasMeasurement ?meas .
      ?meas bigg:measurementFrequency ?timeSeriesFrequency.
      ?meas bigg:hash ?hash.
      ?meas bee:relatestoDeviceName ?deviceName
      FILTER regex(str(?deviceName), "',deviceName,'") .
      ?meas bee:relatestoCommodity ?deviceCommodity .
      FILTER regex(str(?deviceCommodity), "',deviceCommodity,'") 
    }')))
  
  if(nrow(metadata_df)==0){
    return(energyTimeseriesSensor)
  } else {
    metadata_df$deviceCommodity <- gsub(paste0(bigg_namespaces_v2,collapse="|"),"",
                                       metadata_df$deviceCommodity)
    # metadata_df$hash <- mapply(function(i){i[2]},strsplit(metadata_df$hash,"#"))
    if(any(metadata_df$timeSeriesFrequency==frequency)){
      metadata_df <- metadata_df[metadata_df$deviceName==deviceName &
                                 metadata_df$timeSeriesFrequency==frequency,]
    } else {
      frequency_ <- unique(metadata_df$timeSeriesFrequency[
        (as.period(metadata_df$timeSeriesFrequency) < as.period(frequency))])
      frequency_ <- frequency_[which.max(as.numeric(as.period(frequency_)))]
      metadata_df <- metadata_df[metadata_df$deviceName==deviceName &
                                 as.period(metadata_df$timeSeriesFrequency)==as.period(frequency_),]
    }
    
    if(!is.list(timeseriesObject)){
      timeseriesObject_ <- get_sensor_file(timeseriesObject,metadata_df$hash) 
    } else {
      timeseriesObject_ <- timeseriesObject[names(timeseriesObject) %in% metadata_df$hash]
    }
    if(length(timeseriesObject_)==0){
      stop(sprintf("Measurement Hash %s (CO2 emission factors of %s) is not available in the time series of the building.",
                   metadata_df$hash, metadata_df$measuredProperty))
    }
    emissions <- timeseriesObject_[metadata_df$hash][[1]]
    emissions$start <- parse_iso_8601(emissions$start)
    emissions$end <- parse_iso_8601(emissions$end)
    emissions <- emissions[order(emissions$start),]
    emissions <- emissions[!duplicated(emissions$start,fromLast=T),]
    emissions <- align_time_grid(
      data = emissions,
      timeColumn = "start",
      outputFrequency = frequency,
      aggregationFunctions = "AVG",
      aggregationFunctionsSuffix = paste0(measuredProperty,"_EnergyEmissionsFactor"),
      tz = lubridate::tz(energyTimeseriesSensor$time))
    emissions <- emissions %>% select(
      "time",
      paste0("AVG_",measuredProperty,"_EnergyEmissionsFactor"))
    energyTimeseriesSensor <- energyTimeseriesSensor %>% 
      left_join(emissions, by="time")
    energyTimeseriesSensor[,"SUM_EnergyEmissions"] <- 
      if(sum(grepl(paste0(measuredProperty,"$"),colnames(energyTimeseriesSensor)),na.rm=T)>1){
        rowSums(energyTimeseriesSensor[,
            grepl(paste0(measuredProperty,"$"),colnames(energyTimeseriesSensor))] *
          energyTimeseriesSensor[,paste0("AVG_",measuredProperty,"_EnergyEmissionsFactor")], na.rm=T)
      } else {
        energyTimeseriesSensor[,
          grepl(paste0(measuredProperty,"$"),colnames(energyTimeseriesSensor))] *
        energyTimeseriesSensor[,paste0("AVG_",measuredProperty,"_EnergyEmissionsFactor")]
      }
    return(energyTimeseriesSensor)
  }
}
